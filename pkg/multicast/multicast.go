package multicast

import (
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
)

type Multicast interface {
	SendToAll(message communication.Message) error
	setDeliverer(deliverer communication.Deliverer)
	getDeliverer() communication.Deliverer
}

type Target struct {
	Id      string
	Address string
	Sender  communication.Sender
}
