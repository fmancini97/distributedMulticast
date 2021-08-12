package multicast

import (
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/localsender"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"log"
)

type bmulticast struct {
	targets   []Target
	deliverer communication.Deliverer
	mtId      string
	clientId  string
}

func (bm *bmulticast) getClientId() (string, error) {
	return bm.clientId, nil
}

func (bm *bmulticast) SendToAll(message communication.Message) error {
	for _, t := range bm.targets {
		err := t.Sender.Send(bm.mtId, message)
		if err != nil {
			return err
		}
	}
	return nil
}

/*func (bm *bmulticast) SendToAll(message communication.Message) error {

	responseCh := make(chan sendResult)

	for _, t := range bm.targets {
		data := sendData{
			message:  message,
			streamId: bm.mtId,
			tg:       t,
			outputCh: responseCh,
		}
		pool.messageCh <- data
	}

	for i := 0; i < len(bm.targets); i++ {
		result := <-responseCh
		if result.err != nil {
			close(responseCh)
			return errors.New(fmt.Sprintf("Address: %s - Error: %s", result.address, result.err.Error()))
		}
	}
	close(responseCh)

	return nil
}*/

func (bm *bmulticast) Deliver(message communication.Message) error {
	log.Printf("New message: %s\n", message)
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
