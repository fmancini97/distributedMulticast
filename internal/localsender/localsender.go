package localsender

import (
	"errors"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
)

type Sender struct {
	deliverer communication.Deliverer
}

func (ls *Sender) Send(streamId string, message communication.Message) error {
	if ls.deliverer == nil {
		return errors.New("closed")
	}
	return ls.deliverer.Deliver(message)

}

func (ls *Sender) Close() error {
	ls.deliverer = nil
	return nil
}

func New(deliverer communication.Deliverer) *Sender {
	return &Sender{deliverer: deliverer}
}
