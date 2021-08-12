package localsender

import (
	"errors"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
)

type LocalSender struct {
	deliverer communication.Deliverer
}

func (ls *LocalSender) Send(streamId string, message communication.Message) error {
	if ls.deliverer == nil {
		return errors.New("closed")
	}
	return ls.deliverer.Deliver(message)

}

func (ls *LocalSender) Close() error {
	ls.deliverer = nil
	return nil
}

func New(deliverer communication.Deliverer) *LocalSender {
	return &LocalSender{deliverer: deliverer}
}
