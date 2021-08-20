package deliverer

import "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"

type ChanDeliverer struct {
	ch chan communication.Message
}

func (s *ChanDeliverer) Deliver(message communication.Message) error {
	s.ch <- message
	return nil
}

func NewChanDeliverer(ch chan communication.Message) *ChanDeliverer {
	s := new(ChanDeliverer)
	s.ch = ch
	return s
}
