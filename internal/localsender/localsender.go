package localsender

import (
	"errors"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
	"math/rand"
	"sync"
	"time"
)

type Sender struct {
	deliverer communication.Deliverer
	delay     int // Millisecond
	rnd       *rand.Rand
	rdnMu     sync.Mutex
}

func (ls *Sender) Send(_ string, message communication.Message) error {
	if ls.deliverer == nil {
		return errors.New("closed")
	}

	if ls.delay > 0 {
		ls.rdnMu.Lock()
		delayDuration := time.Duration(ls.rnd.Intn(ls.delay))
		ls.rdnMu.Unlock()

		time.Sleep(delayDuration * time.Millisecond)
	}

	return ls.deliverer.Deliver(message)

}

func (ls *Sender) Close() error {
	ls.deliverer = nil
	return nil
}

func New(deliverer communication.Deliverer) *Sender {
	return NewWithDelay(deliverer, 0)
}

func NewWithDelay(deliverer communication.Deliverer, delay int) *Sender {
	var r *rand.Rand
	if delay > 0 {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	return &Sender{deliverer: deliverer, rnd: r, delay: delay}
}
