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

type FifoMulticast struct {
	streamId    string
	clientId    string
	counter     uint64
	targets     map[string]*target
	deliverer   communication.Deliverer
	multicaster Multicast
}

type target struct {
	id            string
	counter       uint64
	holdBackQueue heap.Interface
	mu            sync.Mutex
}

func (fifom *FifoMulticast) Init(streamId, clientId string, multicaster Multicast, targets []Target) {

	targetsMeta := make(map[string]*target)

	for _, t := range targets {
		pq := make(priorityqueue.PriorityQueue, 0)
		heap.Init(&pq)

		targetData := &target{id: t.Id, holdBackQueue: &pq}

		targetsMeta[t.Id] = targetData
	}

	pq := make(priorityqueue.PriorityQueue, 0)
	heap.Init(&pq)

	targetsMeta[clientId] = &target{id: clientId, holdBackQueue: &pq}

	deliverer := multicaster.getDeliverer()

	fifom.streamId = streamId
	fifom.clientId = clientId
	fifom.counter = 0
	fifom.targets = targetsMeta
	fifom.deliverer = deliverer
	fifom.multicaster = multicaster

	multicaster.setDeliverer(fifom)
}

func (fifom *FifoMulticast) SendToAll(message communication.Message) error {
	counter := atomic.AddUint64(&fifom.counter, 1)

	message.Metadata[fmt.Sprintf("%s:counter", fifoHeader)] = strconv.FormatUint(counter, 10)

	message.Metadata[fmt.Sprintf("%s:clientId", fifoHeader)] = fifom.clientId

	return fifom.multicaster.SendToAll(message)
}

func (fifom *FifoMulticast) Deliver(message communication.Message) error {

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

		for i := 0; i < targetData.holdBackQueue.Len(); i++ {
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

func (fifom FifoMulticast) setDeliverer(deliverer communication.Deliverer) {
	fifom.deliverer = deliverer
}

func (fifom FifoMulticast) getDeliverer() communication.Deliverer {
	return fifom.deliverer
}
