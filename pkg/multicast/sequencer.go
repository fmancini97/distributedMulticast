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
	"sync/atomic"
)

const seqHeader = "seq-multicast"

type seqMessageType string

const (
	seqData  seqMessageType = "seqData"
	seqOrder seqMessageType = "seqOrder"
)

var (
	seqLog *log.Entry
)

func init() {
	seqLog = log.WithField("layer", "sequencerMulticast")
}

type seqMtItem struct {
	message        *communication.Message
	sequenceNumber *uint64
}

func (seqItem *seqMtItem) updateSeqMtItem(message communication.Message) error {
	messageType, ok := message.Metadata[fmt.Sprintf("%s:messageType", seqHeader)]

	if !ok {
		return errors.New("missingMessageType")
	}

	if seqMessageType(messageType) != seqOrder && seqMessageType(messageType) != seqData {
		return errors.New("invalidMessageType")
	}

	switch seqMessageType(messageType) {
	case seqOrder:
		seqNumString, ok := message.Metadata[fmt.Sprintf("%s:seqOrder", seqHeader)]
		if !ok {
			return errors.New("missingSeqNumber")
		}

		seqNum, err := strconv.ParseUint(seqNumString, 10, 64)

		if err != nil {
			return err
		}

		seqItem.sequenceNumber = &seqNum
	case seqData:
		seqItem.message = &message
	}

	return nil
}

func (seqItem *seqMtItem) isReady() bool {
	return seqItem.sequenceNumber != nil && seqItem.message != nil
}

type seqMulticast struct {
	mtId          string
	clientId      string
	group         []string
	localCounter  uint64
	globalCounter uint64
	holdBackQueue heap.Interface
	waitingItems  map[string]*seqMtItem
	mu            sync.Mutex
	deliverer     communication.Deliverer
	multicaster   Multicast
}

func (seq *seqMulticast) getGroup() ([]string, error) {
	return seq.group, nil
}

func (seq *seqMulticast) getMTId() (string, error) {
	return seq.mtId, nil
}

func (seq *seqMulticast) getClientId() (string, error) {
	return seq.clientId, nil
}

func (seq *seqMulticast) decorate(deliverer communication.Deliverer) (communication.Deliverer, error) {
	oldDeliverer := seq.deliverer
	seq.deliverer = deliverer
	return oldDeliverer, nil
}

func (seq *seqMulticast) Init(multicaster Multicast) error {

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

	deliverer, err := multicaster.decorate(seq)

	if err != nil {
		return err
	}

	seq.mtId = mtId
	seq.clientId = clientId
	seq.localCounter = 0
	seq.globalCounter = 0
	seq.group = group
	seq.holdBackQueue = priorityqueue.New(func(i interface{}, i2 interface{}) bool {
		return i.(uint64) < i2.(uint64)
	})
	heap.Init(seq.holdBackQueue)
	seq.waitingItems = make(map[string]*seqMtItem)
	seq.deliverer = deliverer
	seq.multicaster = multicaster

	return nil

}

func (seq *seqMulticast) SendToAll(message communication.Message) error {
	counter := atomic.AddUint64(&seq.localCounter, 1)
	messageId := fmt.Sprintf("%s-%d", seq.clientId, counter)
	message.Metadata[fmt.Sprintf("%s:messageId", seqHeader)] = messageId
	message.Metadata[fmt.Sprintf("%s:messageType", seqHeader)] = string(seqData)

	seqLog.WithFields(log.Fields{"client_id": seq.clientId, "counter": counter, "message_id": messageId, "message": message, "method": "SendToAll"}).Debug("Multicasting message")

	return seq.multicaster.SendToAll(message)
}

func (seq *seqMulticast) Deliver(message communication.Message) error {

	logger := seqLog.WithField("method", "Deliver")

	messageId, ok := message.Metadata[fmt.Sprintf("%s:messageId", seqHeader)]

	if !ok {
		logger.WithField("message", message).Error("Missing messageId")
		return errors.New("missingMessageId")
	}

	logger = logger.WithField("messageId", messageId)
	logger.WithField("message", message).Debug("New message")

	seq.mu.Lock()
	defer seq.mu.Unlock()

	waitItem, ok := seq.waitingItems[messageId]

	if !ok {
		waitItem = new(seqMtItem)
		seq.waitingItems[messageId] = waitItem
	}

	err := waitItem.updateSeqMtItem(message)
	if err != nil {
		logger.WithField("error", err).Error("Error while processing message")
		return err
	}

	if !waitItem.isReady() {
		logger.WithField("waitItem", waitItem).Debug("Waiting for Data")
		return nil
	}

	logger.Debug("Delivering messages")

	delete(seq.waitingItems, messageId)

	message = *waitItem.message
	message.Metadata[fmt.Sprintf("%s:seqOrder", seqHeader)] = strconv.FormatUint(*waitItem.sequenceNumber, 10)

	heap.Push(seq.holdBackQueue, &priorityqueue.Item{Value: message, Priority: *waitItem.sequenceNumber})

	numElems := seq.holdBackQueue.Len()

	for i := 0; i < numElems; i++ {
		element := heap.Pop(seq.holdBackQueue).(*priorityqueue.Item)
		if element.Priority == seq.globalCounter+1 {
			message = element.Value.(communication.Message)
			logger.WithFields(log.Fields{"message": message, "counter": element.Priority}).Debug("Delivering message")
			err = seq.deliverer.Deliver(message)
			if err != nil {
				return err
			}
			seq.globalCounter++
		} else if element.Priority.(uint64) > seq.globalCounter+1 {
			heap.Push(seq.holdBackQueue, element)
			break
		}

	}

	return nil
}

type seqMulticastMaster struct {
	globalCounter uint64
	seqmt         *seqMulticast
}

func (seq *seqMulticastMaster) getGroup() ([]string, error) {
	return seq.seqmt.getGroup()
}

func (seq *seqMulticastMaster) getMTId() (string, error) {
	return seq.seqmt.getMTId()
}

func (seq *seqMulticastMaster) getClientId() (string, error) {
	return seq.seqmt.getClientId()
}

func (seq *seqMulticastMaster) decorate(deliverer communication.Deliverer) (communication.Deliverer, error) {
	return seq.seqmt.decorate(deliverer)
}

func (seq *seqMulticastMaster) Init(multicaster Multicast) error {

	seqWork := new(seqMulticast)
	err := seqWork.Init(multicaster)
	if err != nil {
		return err
	}

	_, err = multicaster.decorate(seq)
	if err != nil {
		return err
	}

	seq.seqmt = seqWork

	return nil

}

func (seq *seqMulticastMaster) SendToAll(message communication.Message) error {
	return seq.seqmt.SendToAll(message)
}

func (seq *seqMulticastMaster) Deliver(message communication.Message) error {

	logger := seqLog.WithField("method", "deliver")

	messageId, ok := message.Metadata[fmt.Sprintf("%s:messageId", seqHeader)]

	if !ok {
		logger.WithField("message", message).Error("Missing messageId")
		return errors.New("missingMessageId")
	}

	messageType, ok := message.Metadata[fmt.Sprintf("%s:messageType", seqHeader)]

	if !ok {
		logger.WithField("message", message).Error("Missing messageType")
		return errors.New("missingMessageType")
	}

	if seqMessageType(messageType) != seqOrder && seqMessageType(messageType) != seqData {
		logger.WithField("message", message).Error("Invalid messageType")
		return errors.New("invalidMessageType")
	}

	if seqMessageType(messageType) == seqData {
		metadata := make(map[string]string)
		metadata[fmt.Sprintf("%s:messageId", seqHeader)] = messageId
		metadata[fmt.Sprintf("%s:messageType", seqHeader)] = string(seqOrder)
		metadata[fmt.Sprintf("%s:seqOrder", seqHeader)] = strconv.FormatUint(atomic.AddUint64(&seq.globalCounter, 1), 10)
		logger.WithField("seqOrder", metadata).Debug("Sending seqOrder message")
		go func() {
			err := seq.seqmt.multicaster.SendToAll(communication.Message{Metadata: metadata})
			if err != nil {
				logger.WithField("error", err).Error("Error while sending seqOrder message")
			}
		}()
	}

	return seq.seqmt.Deliver(message)
}

func NewSeqMulticast(container *Container, isMaster bool) (*Container, error) {
	if container == nil || container.dlv == nil || container.mlt == nil {
		return nil, errors.New("notInitialized")
	}

	var seqMulticaster Multicast
	if isMaster {
		seqMaster := new(seqMulticastMaster)
		err := seqMaster.Init(container.mlt)
		if err != nil {
			return nil, err
		}
		seqMulticaster = seqMaster
	} else {
		seqWorker := new(seqMulticast)
		err := seqWorker.Init(container.mlt)
		if err != nil {
			return nil, err
		}
		seqMulticaster = seqWorker
	}

	container.mlt = seqMulticaster

	return container, nil

}
