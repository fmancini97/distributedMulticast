package multicast

import (
	"errors"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
)

type Container struct {
	mlt Multicast
	dlv communication.Deliverer
}

func (cnt *Container) SendToAll(message communication.Message) error {
	if cnt.mlt == nil {
		return errors.New("NilPointer")
	}
	return cnt.mlt.SendToAll(message)
}

func (cnt *Container) Close() error {
	if cnt.mlt == nil {
		return errors.New("NilPointer")
	}
	cnt.dlv = nil
	multicaster := cnt.mlt

	cnt.mlt = nil
	return multicaster.Close()

}

func (cnt *Container) getMulticastId() (string, error) {
	if cnt.mlt == nil {
		return "", errors.New("NilPointer")
	}

	return cnt.mlt.getMulticastId()
}

func (cnt *Container) getClientId() (string, error) {
	if cnt.mlt == nil {
		return "", errors.New("NilPointer")
	}

	return cnt.mlt.getClientId()
}

func (cnt *Container) getGroup() ([]string, error) {
	if cnt.mlt == nil {
		return nil, errors.New("NilPointer")
	}

	return cnt.mlt.getGroup()
}

func (cnt *Container) decorate(dlv communication.Deliverer) (communication.Deliverer, error) {
	if cnt.mlt == nil {
		return nil, errors.New("NilPointer")
	}
	return cnt.mlt.decorate(dlv)
}

func (cnt *Container) Deliver(message communication.Message) error {
	if cnt.dlv == nil {
		return errors.New("NilPointer")
	}
	return cnt.dlv.Deliver(message)
}
