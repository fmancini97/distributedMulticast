package multicast

import (
	"container/heap"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/priorityqueue"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"strconv"
	"sync"
)

const scHeader = "sc-multicast"

type scMessageType string

const (
	scData scMessageType = "scData"
	scAck  scMessageType = "scAck"
)

var (
	scLog *log.Entry
)

func init() {
	scLog = log.WithField("layer", "fifomulticast")
}

type scMulticast struct {
	mtId          string
	clientId      string
	numMembers    int
	timestampMu   sync.Mutex
	timestamp     uint64
	holdBackMu    sync.Mutex
	holdBackQueue heap.Interface
	waitingElems  map[string]*scElem
	msgsCounters  map[string]uint64
	deliverer     communication.Deliverer
	multicaster   Multicast
}

type scElem struct {
	timestamp uint64
	clientId  string
	messageId string
	recvAcks  map[string]bool
	message   *communication.Message
}

type scOrder struct {
	timestamp uint64
	clientId  string
}

func (scm *scMulticast) getGroup() ([]string, error) {
	return scm.multicaster.getGroup()
}

func (scm *scMulticast) getMTId() (string, error) {
	return scm.mtId, nil
}

func (scm *scMulticast) getClientId() (string, error) {
	return scm.clientId, nil
}

func (scm *scMulticast) Init(multicaster Multicast) error {

	clientId, err := multicaster.getClientId()
	if err != nil {
		return err
	}

	mtId, err := multicaster.getMTId()
	if err != nil {
		return err
	}

	group, err := multicaster.getGroup()
	if err != nil {
		return err
	}

	msgCntrs := make(map[string]uint64)

	for _, member := range group {
		msgCntrs[member] = 0
	}

	deliverer, err := multicaster.decorate(scm)

	if err != nil {
		return err
	}

	holdBackQueue := priorityqueue.New(func(i interface{}, i2 interface{}) bool {
		sco1 := i.(scOrder)
		sco2 := i2.(scOrder)
		if sco1.timestamp == sco2.timestamp {
			return sco1.clientId < sco2.clientId
		} else {
			return sco1.timestamp < sco2.timestamp
		}
	})

	heap.Init(holdBackQueue)

	scm.mtId = mtId
	scm.clientId = clientId
	scm.numMembers = len(group)
	scm.holdBackQueue = holdBackQueue
	scm.waitingElems = make(map[string]*scElem)
	scm.msgsCounters = msgCntrs
	scm.deliverer = deliverer
	scm.multicaster = multicaster

	return nil

}

func (scm *scMulticast) SendToAll(message communication.Message) error {

	scm.timestampMu.Lock()
	scm.timestamp++
	timestamp := scm.timestamp
	scm.timestampMu.Unlock()

	timestampString := strconv.FormatUint(timestamp, 10)

	message.Metadata[fmt.Sprintf("%s:timestamp", scHeader)] = timestampString

	message.Metadata[fmt.Sprintf("%s:clientId", scHeader)] = scm.clientId

	message.Metadata[fmt.Sprintf("%s:messageId", scHeader)] = fmt.Sprintf("%s-%s", scm.clientId, timestampString)

	message.Metadata[fmt.Sprintf("%s:messageType", scHeader)] = string(scData)

	scLog.WithFields(log.Fields{"client_id": scm.clientId, "timestamp": timestamp, "message": message}).Debug("Multicasting message")

	return scm.multicaster.SendToAll(message)
}

func (scm *scMulticast) Deliver(message communication.Message) error {

	scLog.WithField("message", message).Debug("New message received")

	timestamp, err := strconv.ParseUint(message.Metadata[fmt.Sprintf("%s:timestamp", scHeader)], 10, 64)

	if err != nil {
		return err
	}

	// Update clock
	scm.timestampMu.Lock()
	if timestamp > scm.timestamp {
		scm.timestamp = timestamp
	}
	scm.timestampMu.Unlock()

	sourceId, ok := message.Metadata[fmt.Sprintf("%s:clientId", scHeader)]

	if !ok {
		return errors.New("clientIdNotFound")
	}

	logger := scLog.WithField("sourceId", sourceId)

	messageType, ok := message.Metadata[fmt.Sprintf("%s:messageType", scHeader)]

	if !ok {
		return errors.New("messageTypeNotFound")
	}

	if scMessageType(messageType) != scAck && scMessageType(messageType) != scData {
		return errors.New("invalidMessageType")
	}

	messageId, ok := message.Metadata[fmt.Sprintf("%s:messageId", scHeader)]

	if !ok {
		return errors.New("messageIdNotFound")
	}

	scm.holdBackMu.Lock()
	defer scm.holdBackMu.Unlock()

	scElm, ok := scm.waitingElems[messageId]

	if !ok {
		scElm = new(scElem)
		scElm.recvAcks = make(map[string]bool)
		scm.waitingElems[messageId] = scElm
	}

	switch scMessageType(messageType) {
	case scData:
		sourceMsgTimestamp := scm.msgsCounters[sourceId]
		if sourceMsgTimestamp < timestamp {
			scm.msgsCounters[sourceId] = timestamp
		}

		scElm.message = &message
		scElm.timestamp = timestamp
		scElm.clientId = sourceId
		scElm.messageId = messageId

		scm.holdBackQueue.Push(&priorityqueue.Item{Value: scElm, Priority: scOrder{timestamp: timestamp, clientId: sourceId}})

		metadata := make(map[string]string)

		scm.timestampMu.Lock()
		scm.timestamp++
		ackTimestamp := scm.timestamp
		scm.timestampMu.Unlock()

		metadata[fmt.Sprintf("%s:timestamp", scHeader)] = strconv.FormatUint(ackTimestamp, 10)

		metadata[fmt.Sprintf("%s:clientId", scHeader)] = scm.clientId

		metadata[fmt.Sprintf("%s:messageId", scHeader)] = messageId

		metadata[fmt.Sprintf("%s:messageType", scHeader)] = string(scAck)

		go func() {
			err = scm.multicaster.SendToAll(communication.Message{Metadata: metadata})
			if err != nil {
				logger.WithField("error", err).Error("Error while sending ack")
			}
		}()

	case scAck:
		scElm.recvAcks[sourceId] = true
	}

	numElem := scm.holdBackQueue.Len()

	for i := 0; i < numElem; i++ {
		element := heap.Pop(scm.holdBackQueue).(*priorityqueue.Item)

		for _, msgCounter := range scm.msgsCounters {
			if msgCounter <= element.Priority.(scOrder).timestamp {
				heap.Push(scm.holdBackQueue, element)
				return nil
			}
		}

		scElm = element.Value.(*scElem)

		delete(scm.waitingElems, scElm.messageId)

		logger.WithFields(log.Fields{"message": *scElm.message, "timestamp": scElm.timestamp}).Debug("Delivering message")
		err = scm.deliverer.Deliver(*scElm.message)
		if err != nil {
			return err
		}

	}

	return nil
}

func (scm *scMulticast) decorate(deliverer communication.Deliverer) (communication.Deliverer, error) {
	oldDeliverer := scm.deliverer
	scm.deliverer = deliverer
	return oldDeliverer, nil
}

func NewSCMulticast(container *Container) (*Container, error) {
	if container == nil || container.dlv == nil || container.mlt == nil {
		return nil, errors.New("notInitialized")
	}

	scMulticaster := new(scMulticast)

	err := scMulticaster.Init(container.mlt)
	if err != nil {
		return nil, err
	}

	container.mlt = scMulticaster

	return container, nil

}
