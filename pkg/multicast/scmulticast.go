package multicast

import (
	"container/heap"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distributedMulticast/internal/priorityqueue"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
	"strconv"
	"sync"
)

const scHeader = "sc-multicast"

var (
	scLog *log.Entry
)

func init() {
	scLog = log.WithField("layer", "sc_multicast")
}

type scMessageType int

const (
	scData scMessageType = iota
	scAck
)

var (
	scMessageTypeName = map[scMessageType]string{
		scData: "scData",
		scAck:  "scAck",
	}

	scMessageTypeValue = map[string]scMessageType{
		"scData": scData,
		"scAck":  scAck,
	}
)

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

type scMulticast struct {
	multicastId     string
	clientId        string
	numMembers      int
	timestampMu     sync.Mutex
	timestamp       uint64
	holdBackMu      sync.Mutex
	holdBackQueue   heap.Interface
	waitingElems    map[string]*scElem
	msgsCounters    map[string]uint64
	deliverer       communication.Deliverer
	fifoMulticaster *fifoMulticast
}

func (scm *scMulticast) SendToAll(message communication.Message) error {

	scm.timestampMu.Lock()
	scm.timestamp++
	timestampScm := scm.timestamp
	scm.fifoMulticaster.counter++
	timestampFifo := scm.fifoMulticaster.counter
	scm.timestampMu.Unlock()

	timestampString := strconv.FormatUint(timestampScm, 10)

	message.Metadata[fmt.Sprintf("%s:timestamp", scHeader)] = timestampString

	message.Metadata[fmt.Sprintf("%s:clientId", scHeader)] = scm.clientId

	message.Metadata[fmt.Sprintf("%s:messageId", scHeader)] = fmt.Sprintf("%s-%s", scm.clientId, timestampString)

	message.Metadata[fmt.Sprintf("%s:messageType", scHeader)] = scMessageTypeName[scData]

	scLog.WithFields(log.Fields{
		"method":      "SendToAll",
		"multicastId": scm.multicastId,
		"client_id":   scm.clientId,
		"timestamp":   timestampScm,
		"metadata":    message.Metadata,
		"payload":     message.Payload,
	}).Debug("Multicasting message")

	message.Metadata[fmt.Sprintf("%s:counter", fifoHeader)] = strconv.FormatUint(timestampFifo, 10)

	message.Metadata[fmt.Sprintf("%s:clientId", fifoHeader)] = scm.clientId

	fifoLog.WithFields(log.Fields{
		"method":      "SendToAll",
		"multicastId": scm.multicastId,
		"client_id":   scm.clientId,
		"counter":     timestampFifo,
		"metadata":    message.Metadata,
		"payload":     message.Payload,
	}).Debug("Multicasting message")

	return scm.fifoMulticaster.multicaster.SendToAll(message)
}

func (scm *scMulticast) Close() error {
	scLog.WithFields(log.Fields{
		"method":      "Close",
		"multicastId": scm.multicastId,
	}).Debug("Closing multicaster")

	scm.multicastId = ""
	scm.clientId = ""
	scm.numMembers = 0
	scm.holdBackQueue = nil
	scm.waitingElems = nil
	scm.msgsCounters = nil
	scm.deliverer = nil

	fifoMulticaster := scm.fifoMulticaster
	scm.fifoMulticaster = nil

	return fifoMulticaster.Close()
}

func (scm *scMulticast) getMulticastId() (string, error) {
	return scm.multicastId, nil
}

func (scm *scMulticast) getClientId() (string, error) {
	return scm.clientId, nil
}

func (scm *scMulticast) getGroup() ([]string, error) {
	return scm.fifoMulticaster.getGroup()
}

func (scm *scMulticast) decorate(deliverer communication.Deliverer) (communication.Deliverer, error) {
	oldDeliverer := scm.deliverer
	scm.deliverer = deliverer
	return oldDeliverer, nil
}

func (scm *scMulticast) Deliver(message communication.Message) error {

	timestamp, err := strconv.ParseUint(message.Metadata[fmt.Sprintf("%s:timestamp", scHeader)], 10, 64)

	if err != nil {
		return err
	}

	sourceId, ok := message.Metadata[fmt.Sprintf("%s:clientId", scHeader)]

	if !ok {
		return errors.New("scMulticast: clientId not found")
	}

	logger := scLog.WithFields(log.Fields{
		"method":      "Deliver",
		"multicastId": scm.multicastId,
		"sourceId":    sourceId,
	})

	logger.WithFields(log.Fields{
		"metadata": message.Metadata,
		"payload":  message.Payload,
	}).Debug("New message received")

	// Update clock
	scm.timestampMu.Lock()
	if timestamp > scm.timestamp {
		logger.Debug("Updating clock")
		scm.timestamp = timestamp
	}
	scm.timestamp++
	scm.timestampMu.Unlock()

	messageType, ok := message.Metadata[fmt.Sprintf("%s:messageType", scHeader)]

	if !ok {
		return errors.New("scMulticast: missing message type")
	}

	if scMessageTypeValue[messageType] != scAck && scMessageTypeValue[messageType] != scData {
		return errors.New("scMulticast: unknown message type")
	}

	messageId, ok := message.Metadata[fmt.Sprintf("%s:messageId", scHeader)]

	if !ok {
		return errors.New("scMulticast: missing message id")
	}

	scm.holdBackMu.Lock()
	defer scm.holdBackMu.Unlock()

	scElm, ok := scm.waitingElems[messageId]

	if !ok {
		logger.Debug("Creating waiting element")
		scElm = new(scElem)
		scElm.recvAcks = make(map[string]bool)
		scm.waitingElems[messageId] = scElm
	}

	sourceMsgTimestamp := scm.msgsCounters[sourceId]
	if sourceMsgTimestamp < timestamp {
		logger.Debug("Updating last timestamp received from source node")
		scm.msgsCounters[sourceId] = timestamp
	}

	switch scMessageTypeValue[messageType] {
	case scData:

		logger.Debug("Processing data message")

		scElm.message = &message
		scElm.timestamp = timestamp
		scElm.clientId = sourceId
		scElm.messageId = messageId

		logger.Debug("Putting message in the hold-back queue")
		scm.holdBackQueue.Push(&priorityqueue.Item{Value: scElm, Priority: scOrder{timestamp: timestamp, clientId: sourceId}})

		go func() {

			ackMessage := communication.NewMessage(nil)

			scm.timestampMu.Lock()
			scm.timestamp++
			ackTimestamp := scm.timestamp
			scm.fifoMulticaster.counter++
			timestampFifo := scm.fifoMulticaster.counter
			scm.timestampMu.Unlock()

			ackMessage.Metadata[fmt.Sprintf("%s:timestamp", scHeader)] = strconv.FormatUint(ackTimestamp, 10)

			ackMessage.Metadata[fmt.Sprintf("%s:clientId", scHeader)] = scm.clientId

			ackMessage.Metadata[fmt.Sprintf("%s:messageId", scHeader)] = messageId

			ackMessage.Metadata[fmt.Sprintf("%s:messageType", scHeader)] = scMessageTypeName[scAck]

			logger.WithField("ack", ackMessage.Metadata).Debug("Multicasting ack")

			ackMessage.Metadata[fmt.Sprintf("%s:counter", fifoHeader)] = strconv.FormatUint(timestampFifo, 10)

			ackMessage.Metadata[fmt.Sprintf("%s:clientId", fifoHeader)] = scm.clientId

			fifoLog.WithFields(log.Fields{
				"client_id": scm.clientId,
				"counter":   timestampFifo,
				"metadata":  ackMessage.Metadata,
				"payload":   ackMessage.Payload,
			}).Debug("Multicasting message")

			err = scm.fifoMulticaster.multicaster.SendToAll(ackMessage)
			if err != nil {
				logger.WithField("error", err).Error("Error while sending ack")
			}

			logger.Debug("Ack sent")
		}()

	case scAck:
		logger.Debug("Processing ack message")

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

		logger.WithFields(log.Fields{
			"timestamp": scElm.timestamp,
			"metadata":  (*scElm.message).Metadata,
			"payload":   (*scElm.message).Payload,
		}).Debug("Delivering message")
		err = scm.deliverer.Deliver(*scElm.message)

		if err != nil {
			return err
		}

	}

	logger.Debug("Message processed")

	return nil
}

func (scm *scMulticast) Init(multicaster Multicast) error {

	clientId, err := multicaster.getClientId()
	if err != nil {
		return err
	}

	mtId, err := multicaster.getMulticastId()
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

	fifoMulticaster := new(fifoMulticast)

	err = fifoMulticaster.Init(multicaster)
	if err != nil {
		return err
	}

	deliverer, err := fifoMulticaster.decorate(scm)

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

	scm.multicastId = mtId
	scm.clientId = clientId
	scm.numMembers = len(group)
	scm.holdBackQueue = holdBackQueue
	scm.waitingElems = make(map[string]*scElem)
	scm.msgsCounters = msgCntrs
	scm.deliverer = deliverer
	scm.fifoMulticaster = fifoMulticaster

	return nil

}

func DecorateSCMulticast(container *Container) (*Container, error) {
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

func NewSCMulticast(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	container, err := NewBMulticast(mtId, clientId, deliverer, targets)

	if err != nil {
		return nil, err
	}
	return DecorateSCMulticast(container)
}
