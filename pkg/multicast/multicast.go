// Package multicast defines interface for multicast operations and it implements multicast algorithms
package multicast

import (
	"errors"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
)

type Multicast interface {
	SendToAll(message communication.Message) error
	Close() error
	getMulticastId() (string, error)
	getClientId() (string, error)
	getGroup() ([]string, error)
	decorate(dlv communication.Deliverer) (communication.Deliverer, error)
}

type E2EMember struct {
	Id      string
	Address string
	Sender  communication.Sender
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
