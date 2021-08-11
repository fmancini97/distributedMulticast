package multicast

import dom "gitlab.com/f_mancini/distribuitedOrderedMulticast"

type Multicast interface {
	SendToAll(message dom.Message) error
}

type Target struct {
	Address string
	Sender  dom.Sender
}
