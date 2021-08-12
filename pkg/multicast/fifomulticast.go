package multicast

import (
	"container/heap"
	"errors"
	"fmt"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/priorityqueue"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
)

const fifoHeader = "fifo-multicast"

type fifoMulticast struct {
	mtId        string
	clientId    string
	counter     uint64
	targets     map[string]*target
	deliverer   communication.Deliverer
	multicaster Multicast
}

func (fifom *fifoMulticast) getGroup() ([]string, error) {
	group := make([]string, 0)
	for _, elem := range fifom.targets {
		group = append(group, elem.id)
	}

	return group, nil
}

func (fifom *fifoMulticast) getMTId() (string, error) {
	return fifom.mtId, nil
}

func (fifom *fifoMulticast) getClientId() (string, error) {
	return fifom.clientId, nil
}

type target struct {
	id            string
	counter       uint64
	holdBackQueue *priorityqueue.PriorityQueue
	mu            sync.Mutex
}

func (fifom *fifoMulticast) Init(multicaster Multicast) error {

	clientId, err := multicaster.getClientId()
	if err != nil {
		return err
	}

	mtId, err := multicaster.getMTId()
	if err != nil {
		return err
	}

	targetsMeta := make(map[string]*target)

	group, err := multicaster.getGroup()
	if err != nil {
		return err
	}

	for _, member := range group {
		pq := make(priorityqueue.PriorityQueue, 0)
		heap.Init(&pq)

		targetData := &target{id: member, holdBackQueue: &pq}

		targetsMeta[member] = targetData
	}

	deliverer, err := multicaster.decorate(fifom)

	if err != nil {
		return err
	}

	fifom.mtId = mtId
	fifom.clientId = clientId
	fifom.counter = 0
	fifom.targets = targetsMeta
	fifom.deliverer = deliverer
	fifom.multicaster = multicaster

	return nil

}

func (fifom *fifoMulticast) SendToAll(message communication.Message) error {
	counter := atomic.AddUint64(&fifom.counter, 1)

	message.Metadata[fmt.Sprintf("%s:counter", fifoHeader)] = strconv.FormatUint(counter, 10)

	message.Metadata[fmt.Sprintf("%s:clientId", fifoHeader)] = fifom.clientId

	return fifom.multicaster.SendToAll(message)
}

func (fifom *fifoMulticast) Deliver(message communication.Message) error {

	log.Printf("FIFO - New message: %s\n", message)

	counter, err := strconv.ParseUint(message.Metadata[fmt.Sprintf("%s:counter", fifoHeader)], 10, 64)

	if err != nil {
		return err
	}

	targetData, ok := fifom.targets[message.Metadata[fmt.Sprintf("%s:clientId", fifoHeader)]]

	if !ok {
		return errors.New("NotFound")
	}

	targetData.mu.Lock()
	defer targetData.mu.Unlock()

	if counter == targetData.counter+1 {
		err := fifom.deliverer.Deliver(message)
		if err != nil {
			return err
		}
		targetData.counter++

		len := targetData.holdBackQueue.Len()

		for i := 0; i < len; i++ {
			element := heap.Pop(targetData.holdBackQueue).(*priorityqueue.Item)
			if element.Priority == targetData.counter+1 {
				err := fifom.deliverer.Deliver(element.Value.(communication.Message))
				if err != nil {
					return err
				}
				targetData.counter++

			} else {
				heap.Push(targetData.holdBackQueue, element)
				break
			}
		}
	} else {
		heap.Push(targetData.holdBackQueue, &priorityqueue.Item{
			Value:    message,
			Priority: counter,
		})
	}

	return nil
}

func (fifom *fifoMulticast) decorate(deliverer communication.Deliverer) (communication.Deliverer, error) {
	oldDeliverer := fifom.deliverer
	fifom.deliverer = deliverer
	return oldDeliverer, nil
}

func NewFifoMulticast(container *Container) (*Container, error) {
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
