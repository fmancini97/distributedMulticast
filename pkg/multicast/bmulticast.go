package multicast

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/localsender"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"math/rand"
)

type bmulticast struct {
	members   []*E2EMember
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
		for index := range rand.Perm(len(bm.members)) {
			data := sendData{
				message:  message,
				streamId: bm.mtId,
				tg:       bm.members[index],
				outputCh: responseCh,
			}
			pool.messageCh <- data
		}
	}()

	var err error
	err = nil

	for i := 0; i < len(bm.members); i++ {
		result := <-responseCh
		if result.err != nil {
			err = errors.New(fmt.Sprintf("Address: %s - Error: %s", result.address, result.err.Error()))
		}
	}

	return err
}

func (bm *bmulticast) Close() error {
	for _, member := range bm.members {
		err := member.Sender.Close()
		if err != nil {
			return err
		}
	}
	bm.mtId = ""
	bm.clientId = ""
	bm.members = nil
	bm.deliverer = nil

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
	for _, t := range bm.members {
		group = append(group, t.Id)
	}
	return group, nil
}

func (bm *bmulticast) getMTId() (string, error) {
	return bm.mtId, nil
}

func (bm *bmulticast) Init(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) {
	targets = append(targets, &E2EMember{clientId, "localhost", localsender.New(bm, 0)})
	bm.members = targets
	bm.deliverer = deliverer
	bm.mtId = mtId
	bm.clientId = clientId
}

func NewBMulticast(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	bm := new(bmulticast)
	bm.Init(mtId, clientId, deliverer, targets)
	container := new(Container)
	container.mlt = bm
	container.dlv = bm
	return container, nil
}
