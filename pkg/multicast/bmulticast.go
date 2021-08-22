package multicast

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distributedMulticast/internal/localsender"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
	"math/rand"
)

// bMulticast implements the basic multicast algorithm
type bMulticast struct {
	multicastId string
	clientId    string
	members     []*E2EMember
	deliverer   communication.Deliverer
}

var (
	bMLog *log.Entry
)

func init() {
	bMLog = log.WithField("layer", "b_multicast")
}

func (bm *bMulticast) SendToAll(message communication.Message) error {

	bMLog.WithFields(log.Fields{
		"method":      "SendToAll",
		"multicastId": bm.multicastId,
		"metadata":    message.Metadata,
		"payload":     message.Payload,
	}).Debug("Multicasting message")

	responseCh := make(chan sendResult)
	defer close(responseCh)

	go func() {
		for index := range rand.Perm(len(bm.members)) {
			data := sendData{
				message:  message,
				streamId: bm.multicastId,
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
			err = errors.New(fmt.Sprintf("bMulticast: Address: %s - Error: %s", result.address, result.err.Error()))
		}
	}

	return err
}

func (bm *bMulticast) Close() error {

	bMLog.WithFields(log.Fields{
		"method":      "Close",
		"multicastId": bm.multicastId,
	}).Debug("Closing multicaster")

	for _, member := range bm.members {
		err := member.Sender.Close()
		if err != nil {
			return err
		}
	}
	bm.multicastId = ""
	bm.clientId = ""
	bm.members = nil
	bm.deliverer = nil

	return nil
}

func (bm *bMulticast) getMulticastId() (string, error) {
	return bm.multicastId, nil
}

func (bm *bMulticast) getClientId() (string, error) {
	return bm.clientId, nil
}

func (bm *bMulticast) getGroup() ([]string, error) {
	group := make([]string, 0)
	for _, t := range bm.members {
		group = append(group, t.Id)
	}
	return group, nil
}

func (bm *bMulticast) decorate(deliverer communication.Deliverer) (communication.Deliverer, error) {
	oldDeliverer := bm.deliverer
	bm.deliverer = deliverer
	return oldDeliverer, nil
}

func (bm *bMulticast) Deliver(message communication.Message) error {
	bMLog.WithFields(log.Fields{
		"method":      "Deliver",
		"multicastId": bm.multicastId,
		"metadata":    message.Metadata,
		"payload":     message.Payload,
	}).Debug("Delivering message")
	return bm.deliverer.Deliver(message)
}

func (bm *bMulticast) Init(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) {
	targets = append(targets, &E2EMember{clientId, "", localsender.New(bm)})
	bm.members = targets
	bm.deliverer = deliverer
	bm.multicastId = mtId
	bm.clientId = clientId
}

func NewBMulticast(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	bm := new(bMulticast)
	bm.Init(mtId, clientId, deliverer, targets)
	container := new(Container)
	container.mlt = bm
	container.dlv = bm
	return container, nil
}
