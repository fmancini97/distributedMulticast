package multicast

import (
	"errors"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
)

type Multicast interface {
	SendToAll(message communication.Message) error
	Close() error
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

func (cnt *Container) Close() error {
	if cnt.mlt == nil {
		return errors.New("NilPointer")
	}
	cnt.dlv = nil
	multicaster := cnt.mlt

	cnt.mlt = nil
	return multicaster.Close()

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
	TypeBMULTICAST Type = iota
	TypeFIFOMULTICAST
	TypeTOMULTICASTSEQMASTER
	TypeTOMULTICASTSEQWORKER
	TypeTOFIFOMULTICASTSEQMASTER
	TypeTOFIFOMULTICASTSEQWORKER
	TypeTOMULTICASTSC
	TypeCOMULTICAST
	TypeTOCOMULTICASTMASTER
	TypeTOCOMULTICASTWORKER
)

var (
	TypeValue = map[string]Type{
		"BMULTICAST":               TypeBMULTICAST,
		"FIFOMULTICAST":            TypeFIFOMULTICAST,
		"TOMULTICASTSEQMASTER":     TypeTOMULTICASTSEQMASTER,
		"TOMULTICASTSEQWORKER":     TypeTOMULTICASTSEQWORKER,
		"TOFIFOMULTICASTSEQMASTER": TypeTOFIFOMULTICASTSEQMASTER,
		"TOFIFOMULTICASTSEQWORKER": TypeTOFIFOMULTICASTSEQWORKER,
		"TOMULTICASTSC":            TypeTOMULTICASTSC,
		"COMULTICAST":              TypeCOMULTICAST,
		"TOCOMULTICASTMASTER":      TypeTOCOMULTICASTMASTER,
		"TOCOMULTICASTWORKER":      TypeTOCOMULTICASTWORKER,
	}

	TypeName = map[Type]string{
		TypeBMULTICAST:               "BMULTICAST",
		TypeFIFOMULTICAST:            "FIFOMULTICAST",
		TypeTOMULTICASTSEQMASTER:     "TOMULTICASTSEQMASTER",
		TypeTOMULTICASTSEQWORKER:     "TOMULTICASTSEQWORKER",
		TypeTOFIFOMULTICASTSEQMASTER: "TOMULTICASTSC",
		TypeTOFIFOMULTICASTSEQWORKER: "COMULTICAST",
		TypeTOMULTICASTSC:            "TOMULTICASTSC",
		TypeCOMULTICAST:              "COMULTICAST",
		TypeTOCOMULTICASTMASTER:      "TOCOMULTICASTMASTER",
		TypeTOCOMULTICASTWORKER:      "TOCOMULTICASTWORKER",
	}
)

var Initializers = map[Type]func(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error){
	TypeBMULTICAST:               NewBMulticast,
	TypeFIFOMULTICAST:            NewFifoMulticast,
	TypeTOMULTICASTSEQMASTER:     NewSeqMaster,
	TypeTOMULTICASTSEQWORKER:     NewSeqWorker,
	TypeTOFIFOMULTICASTSEQMASTER: NewFifoSeqMaster,
	TypeTOFIFOMULTICASTSEQWORKER: NewFifoSeqWorker,
	TypeTOMULTICASTSC:            NewSCMulticast,
	TypeCOMULTICAST:              NewVCMulticast,
	TypeTOCOMULTICASTMASTER:      NewToCoMaster,
	TypeTOCOMULTICASTWORKER:      NewToCoWorker,
}

func New(multicastType Type, mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	initializer, ok := Initializers[multicastType]

	if !ok {
		return nil, errors.New("notImplemented")
	}

	return initializer(mtId, clientId, deliverer, targets)

}
