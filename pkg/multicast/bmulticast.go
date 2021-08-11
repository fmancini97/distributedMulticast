package multicast

import (
	"errors"
	"fmt"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"log"
)

type Bmulticast struct {
	targets   []Target
	deliverer communication.Deliverer
	streamId  string
}

func (bm Bmulticast) SendToAll(message communication.Message) error {

	responseCh := make(chan sendResult)

	for _, t := range bm.targets {
		data := sendData{
			message:  message,
			streamId: bm.streamId,
			tg:       t,
			outputCh: responseCh,
		}
		pool.messageCh <- data
	}

	err := bm.deliverer.Deliver(message)
	if err != nil {
		return err
	}

	for i := 0; i < len(bm.targets); i++ {
		result := <-responseCh
		if result.err != nil {
			close(responseCh)
			return errors.New(fmt.Sprintf("Address: %s - Error: %s", result.address, result.err.Error()))
		}
	}

	return nil
}

func (bm *Bmulticast) Deliver(message communication.Message) error {
	log.Printf("New message: %s\n", message)
	return bm.deliverer.Deliver(message)

}

func (bm *Bmulticast) setDeliverer(deliverer communication.Deliverer) {
	bm.deliverer = deliverer
}

func (bm *Bmulticast) getDeliverer() communication.Deliverer {
	return bm.deliverer
}

func (bm *Bmulticast) Init(streamId string, deliverer communication.Deliverer, targets []Target) {
	bm.targets = targets
	bm.deliverer = deliverer
	bm.streamId = streamId

}
