package bmulticast

import (
	"container/list"
	"errors"
	"fmt"
	dom "gitlab.com/f_mancini/distribuitedOrderedMulticast"
	"sync"
)

type sendData struct {
	message  dom.Message
	streamId string
	tg       *target
	outputCh chan sendResult
}

type multicastPool struct {
	mu         sync.Mutex
	threads    *list.List
	numThreads int
	messageCh  chan sendData
}

const numThreads = 5

var (
	pool multicastPool
)

func init() {
	pool = multicastPool{threads: list.New(), numThreads: numThreads,
		messageCh: make(chan sendData, 50)}

	for i := 0; i < numThreads; i++ {
		quitCh := make(chan bool)

		go processMessages(quitCh, pool.messageCh)

		pool.threads.PushBack(quitCh)

	}

}

func ConfigPool(numThreads int) error {
	if numThreads <= 0 {
		return errors.New("nonPositiveValue")
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	diff := numThreads - pool.numThreads

	if diff >= 0 {
		for i := 0; i < diff; i++ {
			quitCh := make(chan bool)

			go processMessages(quitCh, pool.messageCh)

			pool.threads.PushBack(quitCh)
		}
	} else {
		for i := 0; i < -diff; i++ {
			thread := pool.threads.Front()
			quitCh := thread.Value.(chan bool)
			quitCh <- true
			pool.threads.Remove(thread)
		}
	}

	pool.numThreads = numThreads
	return nil
}

func processMessages(quitCh chan bool, messageCh chan sendData) {
	for {
		select {
		case data := <-messageCh:
			err := data.tg.sender.Send(data.streamId, data.message)

			data.outputCh <- sendResult{address: data.tg.address, err: err}
		case <-quitCh:
			return
		}
	}
}

type Bmulticast struct {
	targets   []*target
	deliverer *dom.Deliverer
	streamId  string
}

type target struct {
	address string
	sender  dom.Sender
}

type sendResult struct {
	address string
	err     error
}

func (bm *Bmulticast) SendToAll(message dom.Message) error {

	responseCh := make(chan sendResult)

	for _, t := range bm.targets {
		data := sendData{
			message:  message,
			streamId: bm.streamId,
			tg:       t,
			outputCh: responseCh,
		}
		pool.messageCh <- data
	}

	for i := 0; i < len(bm.targets); i++ {
		result := <-responseCh
		if result.err != nil {
			close(responseCh)
			return errors.New(fmt.Sprintf("Address: %s - Error: %s", result.address, result.err.Error()))
		}
	}

	return nil
}

func New(streamId string, deliverer *dom.Deliverer, targets []*target) (Bmulticast, error) {
	return Bmulticast{
		targets:   targets,
		deliverer: deliverer,
		streamId:  streamId,
	}, nil
}
