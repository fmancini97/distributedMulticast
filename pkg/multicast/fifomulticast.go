// Package multicast defines interface for multicast operations and it implements multicast algorithms
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
	"sync/atomic"
)

const fifoHeader = "fifo-multicast"

var (
	fifoLog *log.Entry
)

func init() {
	fifoLog = log.WithField("layer", "fifo_multicast")
}

// fifomember maintains the counter and the holdBackQueue of fifo multicast node with identifier id
type fifomember struct {
	id            string
	counter       uint64
	holdBackQueue heap.Interface
	mu            sync.Mutex
}

// fifoMulticast implements FIFO ordering multicast
type fifoMulticast struct {
	multicastId string
	clientId    string
	counter     uint64
	members     map[string]*fifomember
	deliverer   communication.Deliverer
	multicaster Multicast
}

func (fifom *fifoMulticast) SendToAll(message communication.Message) error {

	// Getting counter
	counter := atomic.AddUint64(&fifom.counter, 1)

	// Creating metadata
	message.Metadata[fmt.Sprintf("%s:counter", fifoHeader)] = strconv.FormatUint(counter, 10)

	message.Metadata[fmt.Sprintf("%s:clientId", fifoHeader)] = fifom.clientId

	fifoLog.WithFields(log.Fields{
		"method":      "SendToAll",
		"multicastId": fifom.multicastId,
		"client_id":   fifom.clientId,
		"counter":     counter,
		"metadata":    message.Metadata,
		"payload":     message.Payload}).Debug("Multicasting message")

	// Sending message
	return fifom.multicaster.SendToAll(message)
}

func (fifom *fifoMulticast) Close() error {
	fifoLog.WithFields(log.Fields{
		"method":      "Close",
		"multicastId": fifom.multicastId,
	}).Debug("Closing multicaster")

	fifom.multicastId = ""
	fifom.clientId = ""
	fifom.counter = 0
	fifom.members = nil
	fifom.deliverer = nil

	multicaster := fifom.multicaster
	fifom.multicaster = nil

	return multicaster.Close()
}

func (fifom *fifoMulticast) getMulticastId() (string, error) {
	return fifom.multicastId, nil
}

func (fifom *fifoMulticast) getClientId() (string, error) {
	return fifom.clientId, nil
}

func (fifom *fifoMulticast) getGroup() ([]string, error) {
	group := make([]string, 0)
	for _, elem := range fifom.members {
		group = append(group, elem.id)
	}

	return group, nil
}

func (fifom *fifoMulticast) decorate(deliverer communication.Deliverer) (communication.Deliverer, error) {
	oldDeliverer := fifom.deliverer
	fifom.deliverer = deliverer
	return oldDeliverer, nil
}

func (fifom *fifoMulticast) Deliver(message communication.Message) error {

	// Retrieving counter
	counter, err := strconv.ParseUint(message.Metadata[fmt.Sprintf("%s:counter", fifoHeader)], 10, 64)

	if err != nil {
		return err
	}

	// Retrieving source id
	source, ok := fifom.members[message.Metadata[fmt.Sprintf("%s:clientId", fifoHeader)]]

	if !ok {
		return errors.New("fifoMulticast: source client not found")
	}

	logger := fifoLog.WithFields(log.Fields{
		"method":      "Deliver",
		"multicastId": fifom.multicastId,
		"sourceId":    source.id,
	})

	logger.WithFields(log.Fields{"metadata": message.Metadata, "payload": message.Payload}).Debug("New message received")

	source.mu.Lock()
	defer source.mu.Unlock()

	// Checking if it is the next message to deliver
	if counter == source.counter+1 {
		// Deliver message
		logger.WithFields(log.Fields{"metadata": message.Metadata, "payload": message.Payload, "counter": source.counter}).Debug("Delivering message")
		err := fifom.deliverer.Deliver(message)
		if err != nil {
			return err
		}
		source.counter++

		numElem := source.holdBackQueue.Len()

		// Check if there are other message to deliver
		for i := 0; i < numElem; i++ {
			element := heap.Pop(source.holdBackQueue).(*priorityqueue.Item)
			if element.Priority == source.counter+1 {
				message = element.Value.(communication.Message)
				logger.WithFields(log.Fields{"metadata": message.Metadata, "payload": message.Payload, "counter": source.counter}).Debug("Delivering message")
				err := fifom.deliverer.Deliver(message)
				if err != nil {
					return err
				}
				source.counter++

			} else {
				heap.Push(source.holdBackQueue, element)
				break
			}
		}
	} else if counter > source.counter+1 {
		// Putting message in the hold-back queue
		logger.WithFields(log.Fields{"metadata": message.Metadata, "payload": message.Payload, "counter": source.counter}).Debug("Putting message in the hold-back queue")
		heap.Push(source.holdBackQueue, &priorityqueue.Item{
			Value:    message,
			Priority: counter,
		})
	}

	logger.Debug("Message processed")

	return nil
}

func (fifom *fifoMulticast) Init(multicaster Multicast) error {

	clientId, err := multicaster.getClientId()
	if err != nil {
		return err
	}

	mtId, err := multicaster.getMulticastId()
	if err != nil {
		return err
	}

	targetsMeta := make(map[string]*fifomember)

	group, err := multicaster.getGroup()
	if err != nil {
		return err
	}

	for _, member := range group {
		pq := priorityqueue.New(func(i interface{}, i2 interface{}) bool {
			return i.(uint64) < i2.(uint64)
		})
		heap.Init(pq)

		targetData := &fifomember{id: member, holdBackQueue: pq}

		targetsMeta[member] = targetData
	}

	deliverer, err := multicaster.decorate(fifom)

	if err != nil {
		return err
	}

	fifom.multicastId = mtId
	fifom.clientId = clientId
	fifom.counter = 0
	fifom.members = targetsMeta
	fifom.deliverer = deliverer
	fifom.multicaster = multicaster

	return nil

}

func DecorateFifoMulticast(container *Container) (*Container, error) {
	if container == nil || container.dlv == nil || container.mlt == nil {
		return nil, errors.New("notInitialized")
	}

	fifoMulticaster := new(fifoMulticast)

	err := fifoMulticaster.Init(container.mlt)
	if err != nil {
		return nil, err
	}

	container.mlt = fifoMulticaster

	return container, nil
}

func NewFifoMulticast(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	container, err := NewBMulticast(mtId, clientId, deliverer, targets)

	if err != nil {
		return nil, err
	}
	return DecorateFifoMulticast(container)
}
