package multicast

import (
	"errors"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
)

type Multicast interface {
	SendToAll(message communication.Message) error
	decorate(dlv communication.Deliverer) (communication.Deliverer, error)
	getGroup() ([]string, error)
	getMTId() (string, error)
	getClientId() (string, error)
}

type E2EMember struct {
	Id      string
	Address string
	Sender  communication.Sender
}

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

func (cnt *Container) decorate(dlv communication.Deliverer) (communication.Deliverer, error) {
	if cnt.mlt == nil {
		return nil, errors.New("NilPointer")
	}
	return cnt.mlt.decorate(dlv)
}

func (cnt *Container) getGroup() ([]string, error) {
	if cnt.mlt == nil {
		return nil, errors.New("NilPointer")
	}

	return cnt.mlt.getGroup()
}

func (cnt *Container) getMTId() (string, error) {
	if cnt.mlt == nil {
		return "", errors.New("NilPointer")
	}

	return cnt.mlt.getMTId()
}

func (cnt *Container) getClientId() (string, error) {
	if cnt.mlt == nil {
		return "", errors.New("NilPointer")
	}

	return cnt.mlt.getClientId()
}

func (cnt *Container) Deliver(message communication.Message) error {
	if cnt.dlv == nil {
		return errors.New("NilPointer")
	}
	return cnt.dlv.Deliver(message)
}
