package simpledeliverer

import "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"

type SimpleDeliverer struct {
	ch chan communication.Message
}

func (s *SimpleDeliverer) Deliver(message communication.Message) error {
	s.ch <- message
	return nil
}

func New(ch chan communication.Message) *SimpleDeliverer {
	s := new(SimpleDeliverer)
	s.ch = ch
	return s
}
