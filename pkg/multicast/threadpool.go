package multicast

import (
	"container/list"
	"errors"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"sync"
)

type sendData struct {
	message  communication.Message
	streamId string
	tg       Target
	outputCh chan sendResult
}

type multicastPool struct {
	mu         sync.Mutex
	threads    *list.List
	numThreads int
	messageCh  chan sendData
}

type sendResult struct {
	address string
	err     error
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

// TODO Deadlock: Cambiare!!!!!
func processMessages(quitCh chan bool, messageCh chan sendData) {
	for {
		select {
		case data := <-messageCh:
			err := data.tg.Sender.Send(data.streamId, data.message)

			data.outputCh <- sendResult{address: data.tg.Address, err: err}
		case <-quitCh:
			return
		}
	}
}
