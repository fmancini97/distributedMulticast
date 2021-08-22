package multicast

import (
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
)

type fakeMulticast struct {
	mtId      string
	clientId  string
	group     []string
	deliverer communication.Deliverer
	mtChan    chan communication.Message
}

func (f *fakeMulticast) SendToAll(message communication.Message) error {
	f.mtChan <- message
	return nil
}

func (f *fakeMulticast) Close() error {
	f.mtId = ""
	f.clientId = ""
	f.group = nil
	f.deliverer = nil
	close(f.mtChan)
	f.mtChan = nil
	return nil
}

func (f *fakeMulticast) decorate(dlv communication.Deliverer) (communication.Deliverer, error) {
	oldDeliverer := f.deliverer
	f.deliverer = dlv
	return oldDeliverer, nil
}

func (f *fakeMulticast) getGroup() ([]string, error) {
	return f.group, nil
}

func (f *fakeMulticast) getMulticastId() (string, error) {
	return f.mtId, nil
}

func (f *fakeMulticast) getClientId() (string, error) {
	return f.clientId, nil
}
