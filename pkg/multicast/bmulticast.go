package multicast

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/localsender"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
)

type bmulticast struct {
	targets   []Target
	deliverer communication.Deliverer
	mtId      string
	clientId  string
}

var (
	bmLog *log.Entry
)

func init() {
	bmLog = log.WithField("layer", "bmulticast")
}

func (bm *bmulticast) getClientId() (string, error) {
	return bm.clientId, nil
}

func (bm *bmulticast) SendToAll(message communication.Message) error {

	bmLog.WithField("message", message).Debug("Multicasting message")

	responseCh := make(chan sendResult)
	defer close(responseCh)

	go func() {
		for _, t := range bm.targets {
			data := sendData{
				message:  message,
				streamId: bm.mtId,
				tg:       t,
				outputCh: responseCh,
			}
			pool.messageCh <- data
		}
	}()

	for i := 0; i < len(bm.targets); i++ {
		result := <-responseCh
		if result.err != nil {
			return errors.New(fmt.Sprintf("Address: %s - Error: %s", result.address, result.err.Error()))
		}
	}
	return nil
}

func (bm *bmulticast) Deliver(message communication.Message) error {
	bmLog.WithField("message", message).Debug("Delivering message")
	return bm.deliverer.Deliver(message)
}

func (bm *bmulticast) decorate(deliverer communication.Deliverer) (communication.Deliverer, error) {
	oldDeliverer := bm.deliverer
	bm.deliverer = deliverer
	return oldDeliverer, nil
}

func (bm *bmulticast) getGroup() ([]string, error) {
	group := make([]string, 0)
	for _, t := range bm.targets {
		group = append(group, t.Id)
	}
	return group, nil
}

func (bm *bmulticast) getMTId() (string, error) {
	return bm.mtId, nil
}

func (bm *bmulticast) Init(mtId, clientId string, deliverer communication.Deliverer, targets []Target) {
	targets = append(targets, Target{clientId, "localhost", localsender.New(bm)})
	bm.targets = targets
	bm.deliverer = deliverer
	bm.mtId = mtId
	bm.clientId = clientId
}

func NewBMulticast(mtId, clientId string, deliverer communication.Deliverer, targets []Target) (*Container, error) {
	bm := new(bmulticast)
	bm.Init(mtId, clientId, deliverer, targets)
	container := new(Container)
	container.mlt = bm
	container.dlv = bm
	return container, nil
}
