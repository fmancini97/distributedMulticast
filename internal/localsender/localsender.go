package localsender

import (
	"errors"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"math/rand"
	"time"
)

type Sender struct {
	deliverer communication.Deliverer
	delay     int // Millisecond
	rnd       *rand.Rand
}

func (ls *Sender) Send(_ string, message communication.Message) error {
	if ls.deliverer == nil {
		return errors.New("closed")
	}

	if ls.delay > 0 {
		time.Sleep(time.Duration(ls.rnd.Intn(ls.delay)) * time.Millisecond)
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
