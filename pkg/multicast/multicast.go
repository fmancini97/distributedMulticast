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

type Type int

const (
	Type_BMULTICAST Type = iota
	Type_FIFOMULTICAST
	Type_TOMULTICASTSEQMASTER
	Type_TOMULTICASTSEQWORKER
	Type_TOFIFOMULTICASTSEQMASTER
	Type_TOFIFOMULTICASTSEQWORKER
	Type_TOMULTICASTSC
	Type_COMULTICAST
	Type_TOCOMULTICASTMASTER
	Type_TOCOMULTICASTWORKER
)

var (
	MulticastType_value = map[string]Type{
		"BMULTICAST":               Type_BMULTICAST,
		"FIFOMULTICAST":            Type_FIFOMULTICAST,
		"TOMULTICASTSEQMASTER":     Type_TOMULTICASTSEQMASTER,
		"TOMULTICASTSEQWORKER":     Type_TOMULTICASTSEQWORKER,
		"TOFIFOMULTICASTSEQMASTER": Type_TOFIFOMULTICASTSEQMASTER,
		"TOFIFOMULTICASTSEQWORKER": Type_TOFIFOMULTICASTSEQWORKER,
		"TOMULTICASTSC":            Type_TOMULTICASTSC,
		"COMULTICAST":              Type_COMULTICAST,
		"TOCOMULTICASTMASTER":      Type_TOCOMULTICASTMASTER,
		"TOCOMULTICASTWORKER":      Type_TOCOMULTICASTWORKER,
	}

	MulticastType_name = map[Type]string{
		Type_BMULTICAST:               "BMULTICAST",
		Type_FIFOMULTICAST:            "FIFOMULTICAST",
		Type_TOMULTICASTSEQMASTER:     "TOMULTICASTSEQMASTER",
		Type_TOMULTICASTSEQWORKER:     "TOMULTICASTSEQWORKER",
		Type_TOFIFOMULTICASTSEQMASTER: "TOMULTICASTSC",
		Type_TOFIFOMULTICASTSEQWORKER: "COMULTICAST",
		Type_TOMULTICASTSC:            "TOMULTICASTSC",
		Type_COMULTICAST:              "COMULTICAST",
		Type_TOCOMULTICASTMASTER:      "TOCOMULTICASTMASTER",
		Type_TOCOMULTICASTWORKER:      "TOCOMULTICASTWORKER",
	}
)

var MulticastInitializers = map[Type]func(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error){
	Type_BMULTICAST:               NewBMulticast,
	Type_FIFOMULTICAST:            NewFifoMulticast,
	Type_TOMULTICASTSEQMASTER:     NewSeqMaster,
	Type_TOMULTICASTSEQWORKER:     NewSeqWorker,
	Type_TOFIFOMULTICASTSEQMASTER: NewFifoSeqMaster,
	Type_TOFIFOMULTICASTSEQWORKER: NewSeqWorker,
	Type_TOMULTICASTSC:            NewSCMulticast,
	Type_COMULTICAST:              NewVCMulticast,
	Type_TOCOMULTICASTMASTER:      NewToCoMaster,
	Type_TOCOMULTICASTWORKER:      NewToCoWorker,
}

func New(multicastType Type, mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	initializer, ok := MulticastInitializers[multicastType]

	if !ok {
		return nil, errors.New("notImplemented")
	}

	return initializer(mtId, clientId, deliverer, targets)

}
