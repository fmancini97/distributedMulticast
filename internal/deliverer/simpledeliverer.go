package deliverer

import (
	"errors"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
)

type SimpleDeliverer struct {
	deliverer communication.Deliverer
}

func (lD *SimpleDeliverer) Deliver(message communication.Message) error {
	if lD.deliverer == nil {
		return errors.New("missing deliverer")
	}
	return lD.deliverer.Deliver(message)
}

func (lD *SimpleDeliverer) SetDeliverer(deliverer communication.Deliverer) error {
	lD.deliverer = deliverer
	return nil
}
