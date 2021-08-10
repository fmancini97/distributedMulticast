package multicast

import dom "gitlab.com/f_mancini/distribuitedOrderedMulticast"

type Multicast interface {
	SendToAll(message dom.Message) error
}
