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

var (
	seqLogMaster *log.Entry
	seqLogWorker *log.Entry
)

func init() {
	seqLogMaster = log.WithField("layer", "seqMulticastMaster")
	seqLogWorker = log.WithField("layer", "seqMulticastWorker")
}

type seqMessageType int

const (
	seqData seqMessageType = iota
	seqOrder
)

var (
	seqMessageTypeName = map[seqMessageType]string{
		seqData:  "seqData",
		seqOrder: "seqOrder",
	}

	seqMessageTypeValue = map[string]seqMessageType{
		"seqData":  seqData,
		"seqOrder": seqOrder,
	}
)

type seqMtItem struct {
	message        *communication.Message
	sequenceNumber *uint64
}

func (seqItem *seqMtItem) updateSeqMtItem(message communication.Message) error {
	messageTypeString, ok := message.Metadata[fmt.Sprintf("%s:messageType", seqHeader)]

	if !ok {
		return errors.New("missingMessageType")
	}

	messageType, ok := seqMessageTypeValue[messageTypeString]

	if !ok {
		return errors.New("seqMulticast: unknown message type")
	}

	switch messageType {
	case seqOrder:
		seqNumString, ok := message.Metadata[fmt.Sprintf("%s:seqOrder", seqHeader)]
		if !ok {
			return errors.New("seqMulticast: missing sequence number")
		}

		seqNum, err := strconv.ParseUint(seqNumString, 10, 64)

		if err != nil {
			return errors.New("seqMulticast: " + err.Error())
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
	multicastId   string
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

func (seq *seqMulticast) SendToAll(message communication.Message) error {
	counter := atomic.AddUint64(&seq.localCounter, 1)
	messageId := fmt.Sprintf("%s-%d", seq.clientId, counter)
	message.Metadata[fmt.Sprintf("%s:messageId", seqHeader)] = messageId
	message.Metadata[fmt.Sprintf("%s:messageType", seqHeader)] = seqMessageTypeName[seqData]

	seqLogWorker.WithFields(log.Fields{
		"method":      "SendToAll",
		"multicastId": seq.multicastId,
		"clientId":    seq.clientId,
		"counter":     counter,
		"messageId":   messageId,
		"metadata":    message.Metadata,
		"payload":     message.Payload,
	}).Debug("Multicasting message")

	return seq.multicaster.SendToAll(message)
}

func (seq *seqMulticast) Close() error {
	seqLogWorker.WithFields(log.Fields{
		"method":      "Close",
		"multicastId": seq.multicastId,
	}).Debug("Closing multicaster")

	seq.multicastId = ""
	seq.clientId = ""
	seq.localCounter = 0
	seq.globalCounter = 0
	seq.group = nil
	seq.holdBackQueue = nil
	seq.waitingItems = nil
	seq.deliverer = nil

	multicaster := seq.multicaster
	seq.multicaster = nil

	return multicaster.Close()
}

func (seq *seqMulticast) getMulticastId() (string, error) {
	return seq.multicastId, nil
}

func (seq *seqMulticast) getClientId() (string, error) {
	return seq.clientId, nil
}

func (seq *seqMulticast) getGroup() ([]string, error) {
	return seq.group, nil
}

func (seq *seqMulticast) decorate(deliverer communication.Deliverer) (communication.Deliverer, error) {
	oldDeliverer := seq.deliverer
	seq.deliverer = deliverer
	return oldDeliverer, nil
}

func (seq *seqMulticast) Deliver(message communication.Message) error {
	messageId, ok := message.Metadata[fmt.Sprintf("%s:messageId", seqHeader)]

	if !ok {
		return errors.New("missingMessageId")
	}

	logger := seqLogWorker.WithFields(log.Fields{
		"method":      "Deliver",
		"multicastId": seq.multicastId,
		"messageId":   messageId})

	logger.WithFields(log.Fields{
		"metadata": message.Metadata,
		"payload":  message.Payload,
	}).Debug("New message")

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

	logger.Debug("Putting message in the hold-back queue")

	delete(seq.waitingItems, messageId)

	message = *waitItem.message
	message.Metadata[fmt.Sprintf("%s:seqOrder", seqHeader)] = strconv.FormatUint(*waitItem.sequenceNumber, 10)

	heap.Push(seq.holdBackQueue, &priorityqueue.Item{Value: message, Priority: *waitItem.sequenceNumber})

	numElems := seq.holdBackQueue.Len()

	for i := 0; i < numElems; i++ {
		element := heap.Pop(seq.holdBackQueue).(*priorityqueue.Item)
		if element.Priority == seq.globalCounter+1 {
			message = element.Value.(communication.Message)
			logger.WithFields(log.Fields{
				"counter":  element.Priority,
				"metadata": message.Metadata,
				"payload":  message.Payload,
			}).Debug("Delivering message")

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

	logger.Debug("Message processed")

	return nil
}

func (seq *seqMulticast) Init(multicaster Multicast) error {

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

	deliverer, err := multicaster.decorate(seq)

	if err != nil {
		return err
	}

	seq.multicastId = mtId
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

type seqMulticastMaster struct {
	globalCounter uint64
	seqmt         *seqMulticast
}

func (seq *seqMulticastMaster) SendToAll(message communication.Message) error {

	multicastId, _ := seq.getMulticastId()
	clientId, _ := seq.getClientId()

	seqLogMaster.WithFields(log.Fields{
		"method":      "SendToAll",
		"multicastId": multicastId,
		"clientId":    clientId,
		"metadata":    message.Metadata,
		"payload":     message.Payload,
	}).Debug("Multicasting message")

	return seq.seqmt.SendToAll(message)
}

func (seq *seqMulticastMaster) Close() error {

	multicastId, _ := seq.getMulticastId()

	seqLogMaster.WithFields(log.Fields{
		"method":      "Close",
		"multicastId": multicastId,
	}).Debug("Closing multicaster")

	seq.globalCounter = 0
	seqWorker := seq.seqmt
	seq.seqmt = nil
	return seqWorker.Close()
}

func (seq *seqMulticastMaster) getMulticastId() (string, error) {
	return seq.seqmt.getMulticastId()
}

func (seq *seqMulticastMaster) getClientId() (string, error) {
	return seq.seqmt.getClientId()
}

func (seq *seqMulticastMaster) getGroup() ([]string, error) {
	return seq.seqmt.getGroup()
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

func (seq *seqMulticastMaster) Deliver(message communication.Message) error {

	messageId, ok := message.Metadata[fmt.Sprintf("%s:messageId", seqHeader)]

	if !ok {
		return errors.New("seqMulticast: missing message id")
	}

	multicastId, _ := seq.getMulticastId()

	logger := seqLogMaster.WithFields(log.Fields{
		"method":      "Deliver",
		"multicastId": multicastId,
		"messageId":   messageId,
	})

	messageTypeString, ok := message.Metadata[fmt.Sprintf("%s:messageType", seqHeader)]

	if !ok {
		return errors.New("seqMulticast: missing message type")
	}

	messageType, ok := seqMessageTypeValue[messageTypeString]

	if !ok {
		return errors.New("seqMulticast: unknown message type")
	}

	if messageType == seqData {

		logger.Debug("Processing data message")

		metadata := make(map[string]string)
		metadata[fmt.Sprintf("%s:messageId", seqHeader)] = messageId
		metadata[fmt.Sprintf("%s:messageType", seqHeader)] = seqMessageTypeName[seqOrder]
		metadata[fmt.Sprintf("%s:seqOrder", seqHeader)] = strconv.FormatUint(atomic.AddUint64(&seq.globalCounter, 1), 10)
		go func() {
			logger.WithField("seqOrder", metadata).Debug("Sending order message")

			err := seq.seqmt.multicaster.SendToAll(communication.Message{Metadata: metadata})
			if err != nil {
				logger.WithField("error", err).Error("Error while sending order message")
			}
		}()
	}

	logger.Debug("Message processed")

	return seq.seqmt.Deliver(message)
}

func DecorateSeqMulticast(container *Container, isMaster bool) (*Container, error) {
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

func NewSeqWorker(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	container, err := NewBMulticast(mtId, clientId, deliverer, targets)

	if err != nil {
		return nil, err
	}

	return DecorateSeqMulticast(container, false)
}

func NewSeqMaster(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	container, err := NewBMulticast(mtId, clientId, deliverer, targets)

	if err != nil {
		return nil, err
	}

	return DecorateSeqMulticast(container, true)
}

func NewFifoSeqWorker(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	container, err := NewFifoMulticast(mtId, clientId, deliverer, targets)

	if err != nil {
		return nil, err
	}

	return DecorateSeqMulticast(container, false)
}

func NewFifoSeqMaster(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	container, err := NewFifoMulticast(mtId, clientId, deliverer, targets)

	if err != nil {
		return nil, err
	}

	return DecorateSeqMulticast(container, true)
}

func NewToCoWorker(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	container, err := NewVCMulticast(mtId, clientId, deliverer, targets)

	if err != nil {
		return nil, err
	}

	return DecorateSeqMulticast(container, false)
}

func NewToCoMaster(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	container, err := NewVCMulticast(mtId, clientId, deliverer, targets)

	if err != nil {
		return nil, err
	}

	return DecorateSeqMulticast(container, true)
}
