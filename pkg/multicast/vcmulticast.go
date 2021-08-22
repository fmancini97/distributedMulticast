package multicast

import (
	"bytes"
	"container/heap"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/priorityqueue"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"sync"
	"sync/atomic"
)

const vcHeader = "vc-multicast"

var (
	vcLog *log.Entry
)

func init() {
	vcLog = log.WithField("layer", "vcMulticast")
}

type vcMember struct {
	clientId      string
	mu            sync.Mutex
	holdBackQueue heap.Interface
}

type waitingMessage struct {
	message       communication.Message
	timestamp     map[string]uint64
	missingChecks []string // Client id which does not match the delivery requirements
}

type vcMulticast struct {
	multicastId    string
	clientId       string
	membersId      []string
	localTimestamp uint64
	timestamp      map[string]uint64
	group          map[string]*vcMember
	deliverer      communication.Deliverer
	multicaster    Multicast
}

func (vcm *vcMulticast) SendToAll(message communication.Message) error {

	timestampBytes := new(bytes.Buffer)

	encoder := gob.NewEncoder(timestampBytes)

	localTimestamp := atomic.AddUint64(&vcm.localTimestamp, 1)

	timestampVector := make(map[string]uint64, len(vcm.timestamp))

	for clientId, timestamp := range vcm.timestamp {
		timestampVector[clientId] = timestamp
	}

	timestampVector[vcm.clientId] = localTimestamp

	err := encoder.Encode(timestampVector)

	if err != nil {
		return errors.New("vcMulticast: " + err.Error())
	}

	timestampB64 := base64.StdEncoding.EncodeToString(timestampBytes.Bytes())

	message.Metadata[fmt.Sprintf("%s:timestamp", vcHeader)] = timestampB64

	message.Metadata[fmt.Sprintf("%s:clientId", vcHeader)] = vcm.clientId

	vcLog.WithFields(log.Fields{
		"method":      "SendToAll",
		"multicastId": vcm.multicastId,
		"clientId":    vcm.clientId,
		"timestamp":   timestampVector,
		"metadata":    message.Metadata,
		"payload":     message.Payload,
	}).Debug("Multicasting message")

	return vcm.multicaster.SendToAll(message)
}

func (vcm *vcMulticast) Close() error {
	vcLog.WithFields(log.Fields{
		"method":      "Close",
		"multicastId": vcm.multicastId,
	}).Debug("Closing multicaster")

	vcm.clientId = ""
	vcm.multicastId = ""
	vcm.group = nil
	vcm.membersId = nil
	vcm.timestamp = nil
	multicaster := vcm.multicaster

	vcm.multicaster = nil
	vcm.deliverer = nil

	return multicaster.Close()
}

func (vcm *vcMulticast) getMulticastId() (string, error) {
	return vcm.multicastId, nil
}

func (vcm *vcMulticast) getClientId() (string, error) {
	return vcm.clientId, nil
}

func (vcm *vcMulticast) getGroup() ([]string, error) {
	return vcm.membersId, nil
}

func (vcm *vcMulticast) decorate(deliverer communication.Deliverer) (communication.Deliverer, error) {
	oldDeliverer := vcm.deliverer
	vcm.deliverer = deliverer
	return oldDeliverer, nil
}

func (vcm *vcMulticast) Deliver(message communication.Message) error {

	logger := vcLog.WithFields(log.Fields{
		"method":      "Deliver",
		"multicastId": vcm.multicastId,
	})

	logger.WithFields(log.Fields{
		"metadata": message.Metadata,
		"payload":  message.Payload,
	}).Debug("New message received")

	timestampB64, ok := message.Metadata[fmt.Sprintf("%s:timestamp", vcHeader)]

	if !ok {
		return errors.New("vcMulticast: missing timestamp")
	}

	timestampBytes, err := base64.StdEncoding.DecodeString(timestampB64)

	if err != nil {
		return errors.New("vcMulticast: " + err.Error())
	}

	var timestampVector map[string]uint64

	timestampBuffer := bytes.Buffer{}
	timestampBuffer.Write(timestampBytes)
	decoder := gob.NewDecoder(&timestampBuffer)
	err = decoder.Decode(&timestampVector)

	if err != nil {
		return errors.New("vcMulticast: " + err.Error())

	}

	sourceId, ok := message.Metadata[fmt.Sprintf("%s:clientId", vcHeader)]

	if !ok {
		return errors.New("vcMulticast: missing client id")
	}

	member, ok := vcm.group[sourceId]

	if !ok {
		return errors.New("vcMulticast: unknown client id")
	}

	logger.WithField("clientId", sourceId).Debug("Putting message in the hold-back queue")

	member.mu.Lock()
	heap.Push(member.holdBackQueue, &priorityqueue.Item{Value: &waitingMessage{
		message:       message,
		timestamp:     timestampVector,
		missingChecks: vcm.membersId,
	}, Priority: timestampVector[sourceId]})
	member.mu.Unlock()

	for clientId, member := range vcm.group {

		member.mu.Lock()

		if clientId == vcm.clientId {
			numItems := member.holdBackQueue.Len()
			for i := 0; i < numItems; i++ {
				element := heap.Pop(member.holdBackQueue).(*priorityqueue.Item)
				if element.Priority == vcm.timestamp[vcm.clientId]+1 {

					message := element.Value.(*waitingMessage).message
					logger.WithFields(log.Fields{
						"metadata": message.Metadata,
						"payload":  message.Payload,
					}).Debug("Delivering message")

					err := vcm.deliverer.Deliver(message)
					if err != nil {
						return err
					}
					vcm.timestamp[vcm.clientId]++
				} else {
					heap.Push(member.holdBackQueue, element)
					break
				}
			}
		} else {
			numItems := member.holdBackQueue.Len()
			for i := 0; i < numItems; i++ {
				element := heap.Pop(member.holdBackQueue).(*priorityqueue.Item)
				waitingElem := element.Value.(*waitingMessage)

				missing := make([]string, 0)

				for _, clientId := range waitingElem.missingChecks {
					if (clientId == member.clientId && waitingElem.timestamp[clientId] != vcm.timestamp[clientId]+1) || (clientId != member.clientId && waitingElem.timestamp[clientId] > vcm.timestamp[clientId]) {
						missing = append(missing, clientId)
					}
				}

				if len(missing) == 0 {
					vcLog.WithFields(log.Fields{"clientId": member.clientId, "timestamp": waitingElem.timestamp, "message": waitingElem.message}).Debug("Delivering message")

					logger.WithFields(log.Fields{
						"metadata": waitingElem.message.Metadata,
						"payload":  waitingElem.message.Payload,
					}).Debug("Delivering message")

					err := vcm.deliverer.Deliver(waitingElem.message)
					if err != nil {
						return err
					}

					vcm.timestamp[clientId]++

				} else {
					waitingElem.missingChecks = missing
					heap.Push(member.holdBackQueue, element)
					break
				}
			}
		}

		member.mu.Unlock()

	}

	logger.Debug("Message processed")

	return nil
}

func (vcm *vcMulticast) Init(multicaster Multicast) error {

	clientId, err := multicaster.getClientId()
	if err != nil {
		return err
	}

	mtId, err := multicaster.getMulticastId()
	if err != nil {
		return err
	}

	group, err := multicaster.getGroup()
	if err != nil {
		return err
	}

	vcGroup := make(map[string]*vcMember)
	timestamp := make(map[string]uint64)

	for _, clientId := range group {
		m := new(vcMember)
		m.holdBackQueue = priorityqueue.New(func(i interface{}, i2 interface{}) bool {
			return i.(uint64) < i2.(uint64)
		})
		m.clientId = clientId
		vcGroup[clientId] = m

		timestamp[clientId] = 0

	}

	deliverer, err := multicaster.decorate(vcm)

	if err != nil {
		return err
	}

	vcm.clientId = clientId
	vcm.multicastId = mtId
	vcm.group = vcGroup
	vcm.membersId = group
	vcm.timestamp = timestamp
	vcm.multicaster = multicaster
	vcm.deliverer = deliverer

	return nil

}

func DecorateVCMulticast(container *Container) (*Container, error) {
	if container == nil || container.dlv == nil || container.mlt == nil {
		return nil, errors.New("notInitialized")
	}

	scMulticaster := new(vcMulticast)

	err := scMulticaster.Init(container.mlt)
	if err != nil {
		return nil, err
	}

	container.mlt = scMulticaster

	return container, nil

}

func NewVCMulticast(mtId, clientId string, deliverer communication.Deliverer, targets []*E2EMember) (*Container, error) {
	container, err := NewBMulticast(mtId, clientId, deliverer, targets)

	if err != nil {
		return nil, err
	}
	return DecorateVCMulticast(container)
}
