package multicast

import (
	"encoding/binary"
	"errors"
	"fmt"
	"gitlab.com/f_mancini/distributedMulticast/internal/deliverer"
	"gitlab.com/f_mancini/distributedMulticast/internal/randstring"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSCMulticast_Init(t *testing.T) {

	mtId := "testSCMulticast"
	clientId := randstring.RandSeq(10)

	numMembers := 3

	ch := make(chan communication.Message)
	smplDlv := deliverer.NewChanDeliverer(ch)

	mtChan := make(chan communication.Message)

	members := make([]string, 0)

	for i := 0; i < numMembers-1; i++ {
		members = append(members, randstring.RandSeq(10))
	}

	members = append(members, clientId)

	fakeMt := new(fakeMulticast)
	fakeMt.mtId = mtId
	fakeMt.clientId = clientId
	fakeMt.group = members
	fakeMt.deliverer = smplDlv
	fakeMt.mtChan = mtChan

	scM := new(scMulticast)

	err := scM.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing multicaster: %s", err)
	}

	err = checkSCMulticast(scM, mtId, clientId, smplDlv, members)
	if err != nil {
		t.Fatalf("Error while checking sequecer: %s", err)
	}
}

func checkSCMulticast(scM *scMulticast, mtId string, clientId string, smplDlv communication.Deliverer, members []string) error {
	if mtId != scM.multicastId {
		return errors.New(fmt.Sprintf("Multicast ID mismatch - Expected: %s, Actual: %s", mtId, scM.multicastId))
	}

	if clientId != scM.clientId {
		return errors.New(fmt.Sprintf("Client ID mismatch - Expected: %s, Actual: %s", clientId, scM.clientId))
	}

	if smplDlv != scM.deliverer {
		return errors.New(fmt.Sprintf("Deliverer mismatch"))
	}

	if scM.numMembers != len(members) {
		return errors.New(fmt.Sprintf("Number of members mismatch - Expected: %d, Acutal: %d", len(members), scM.numMembers))
	}

	return checkFifoMulticast(scM.fifoMulticaster, mtId, clientId, scM, members)
}

func TestScMulticast_SendToAll(t *testing.T) {
	mtId := "testScMulticast"
	clientId := randstring.RandSeq(10)

	numMessages := uint64(3)
	numMembers := 3

	ch := make(chan communication.Message)
	smplDlv := deliverer.NewChanDeliverer(ch)

	mtChan := make(chan communication.Message)

	members := make([]string, 0)
	for i := 0; i < numMembers-1; i++ {
		members = append(members, randstring.RandSeq(10))
	}

	members = append(members, clientId)

	fakeMt := new(fakeMulticast)
	fakeMt.mtId = mtId
	fakeMt.clientId = clientId
	fakeMt.group = members
	fakeMt.deliverer = smplDlv
	fakeMt.mtChan = mtChan

	scm := new(seqMulticast)
	err := scm.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing Sequencer: %s", err)
	}

	errCh := make(chan error)

	for counter := uint64(0); counter < numMessages; counter++ {
		counter := counter
		go func() {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, counter+1)
			err := scm.SendToAll(communication.NewMessage(b))
			if err != nil {
				errCh <- err
			}
		}()
	}

	for receivedMsgs := make(map[string]bool); uint64(len(receivedMsgs)) == numMessages; {
		select {
		case message := <-mtChan:
			err := checkScDataMessage(message, scm.clientId)
			if err != nil {
				t.Fatalf("Error while checking message: %s", err)
			}

		case err := <-errCh:
			t.Fatalf("Error while multicasting message: %s", err)
		case <-time.After(time.Second * 60):
			t.Fatalf("Missing message")
		}
	}
}

func checkScDataMessage(message communication.Message, expectedClientId string) error {
	messageType, ok := message.Metadata[fmt.Sprintf("%s:messageType", scHeader)]

	if !ok {
		return errors.New("missing messageType")
	}

	if scMessageTypeValue[messageType] != scData {
		return errors.New(fmt.Sprintf("messageType mismatch - Expected: %s, Actual: %s", scMessageTypeName[scData], messageType))

	}

	timestampString, ok := message.Metadata[fmt.Sprintf("%s:timestamp", scHeader)]

	if !ok {
		return errors.New("missing timestampString")
	}

	clientId, ok := message.Metadata[fmt.Sprintf("%s:clientId", scHeader)]

	if !ok {
		return errors.New("missing clientID")
	}

	if clientId != expectedClientId {
		return errors.New(fmt.Sprintf("clientId mismatch - Expected: %s, Actual: %s", expectedClientId, clientId))
	}

	messageId, ok := message.Metadata[fmt.Sprintf("%s:messageId", scHeader)]

	if !ok {
		return errors.New("missing messageId")
	}

	expectedMessageId := fmt.Sprintf("%s-%s", clientId, timestampString)

	if messageId != expectedMessageId {
		return errors.New(fmt.Sprintf("messageId mismatch - Expected: %s, Actual: %s", expectedMessageId, messageId))

	}

	return checkFifoMessage(message, expectedMessageId)

}

func TestScMulticast_Deliver(t *testing.T) {
	mtId := "testScMulticast"
	clientId := randstring.RandSeq(10)

	numMembers := 3

	ch := make(chan communication.Message)
	smplDlv := deliverer.NewChanDeliverer(ch)

	mtChan := make(chan communication.Message)

	members := make([]string, 0)
	for i := 0; i < numMembers-1; i++ {
		members = append(members, randstring.RandSeq(10))
	}

	members = append(members, clientId)

	fakeMt := new(fakeMulticast)
	fakeMt.mtId = mtId
	fakeMt.clientId = clientId
	fakeMt.group = members
	fakeMt.deliverer = smplDlv
	fakeMt.mtChan = mtChan

	scm := new(scMulticast)
	err := scm.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing sequencer: %s", err)
	}

	errCh := make(chan error)

	metadata := make(map[string]string)

	scTimestamp := rand.Intn(20)

	srcClientId := members[rand.Intn(len(members))]

	messageString := randstring.RandSeq(20)

	timestampString := strconv.FormatUint(uint64(scTimestamp), 10)

	metadata[fmt.Sprintf("%s:messageType", scHeader)] = scMessageTypeName[scData]

	metadata[fmt.Sprintf("%s:timestamp", scHeader)] = timestampString

	metadata[fmt.Sprintf("%s:clientId", scHeader)] = srcClientId

	messageId := fmt.Sprintf("%s-%s", srcClientId, timestampString)

	metadata[fmt.Sprintf("%s:messageId", scHeader)] = messageId

	metadata[fmt.Sprintf("%s:counter", fifoHeader)] = strconv.FormatUint(1, 10)

	metadata[fmt.Sprintf("%s:clientId", fifoHeader)] = srcClientId

	go func() {
		err := scm.fifoMulticaster.Deliver(communication.Message{
			Metadata: metadata,
			Payload:  []byte(messageString),
		})
		if err != nil {
			errCh <- err
		}
	}()

	ackMessages := make(map[string]communication.Message)

	select {
	case message := <-mtChan:
		err = checkScAckMessage(message, scm.clientId, messageId)
		if err != nil {
			t.Fatalf("Error while delivering message: %s", err)
		}
		ackMessages[scm.clientId] = message
	case err := <-errCh:
		t.Fatalf("Error while sending message: %s", err)
	case <-time.After(time.Second * 60):
		t.Fatalf("Ack message not received")
	}

	for _, member := range members {
		if member != scm.clientId {
			metadata := make(map[string]string)

			ackTimestampString := strconv.FormatUint(uint64(scTimestamp+1), 10)

			metadata[fmt.Sprintf("%s:messageType", scHeader)] = scMessageTypeName[scAck]

			metadata[fmt.Sprintf("%s:timestamp", scHeader)] = ackTimestampString

			metadata[fmt.Sprintf("%s:clientId", scHeader)] = member

			messageId := fmt.Sprintf("%s-%s", member, ackTimestampString)

			metadata[fmt.Sprintf("%s:messageId", scHeader)] = messageId

			fifoCounter := uint64(1)

			if srcClientId == member {
				fifoCounter++
			}

			metadata[fmt.Sprintf("%s:counter", fifoHeader)] = strconv.FormatUint(fifoCounter, 10)

			metadata[fmt.Sprintf("%s:clientId", fifoHeader)] = member

			ackMessages[member] = communication.Message{
				Metadata: metadata,
				Payload:  nil,
			}

		}
	}

	for _, ackMessage := range ackMessages {
		ackMessage := ackMessage
		go func() {
			err := scm.fifoMulticaster.Deliver(ackMessage)
			if err != nil {
				errCh <- err
			}
		}()
	}

	select {
	case message := <-ch:
		if string(message.Payload) != messageString {
			t.Fatalf("Message mismatch - Expected: %s, Actual: %s", messageString, string(message.Payload))
		}

	case err := <-errCh:
		t.Fatalf("Error while delivering acks: %s", err)
	case <-time.After(time.Second * 60):
		t.Fatalf("Missing ack message")
	}

}

func checkScAckMessage(message communication.Message, expectedClientId, expectedMessageId string) error {
	messageType, ok := message.Metadata[fmt.Sprintf("%s:messageType", scHeader)]

	if !ok {
		return errors.New("missing messageType")
	}

	if scMessageTypeValue[messageType] != scAck {
		return errors.New(fmt.Sprintf("messageType mismatch - Expected: %s, Actual: %s", scMessageTypeName[scData], messageType))

	}

	_, ok = message.Metadata[fmt.Sprintf("%s:timestamp", scHeader)]

	if !ok {
		return errors.New("missing timestampString")
	}

	clientId, ok := message.Metadata[fmt.Sprintf("%s:clientId", scHeader)]

	if !ok {
		return errors.New("missing clientID")
	}

	if clientId != expectedClientId {
		return errors.New(fmt.Sprintf("clientId mismatch - Expected: %s, Actual: %s", expectedClientId, clientId))
	}

	messageId, ok := message.Metadata[fmt.Sprintf("%s:messageId", scHeader)]

	if !ok {
		return errors.New("missing messageId")
	}

	if messageId != expectedMessageId {
		return errors.New(fmt.Sprintf("messageId mismatch - Expected: %s, Actual: %s", expectedMessageId, messageId))

	}

	return checkFifoMessage(message, expectedClientId)

}

func TestScMulticastSingle(t *testing.T) {
	scMulticaster(t, true)
}

func TestScMulticastMultiple(t *testing.T) {
	scMulticaster(t, false)
}

func scMulticaster(t *testing.T, singleSender bool) {
	numMembers := 3
	delay := 1000
	messageToSent := 100

	err := ConfigPool(messageToSent)
	if err != nil {
		t.Fatalf("Error while configuring pool: %s", err)
	}

	bMulticasters, chDeliverers, err := initTestBMulticast(numMembers, delay)

	if err != nil {
		t.Fatalf("Error while initialializing BMultitcast: %s", err)
	}

	errCh := make(chan error)

	counter := uint64(0)

	scMulticasters := make(map[string]*Container)

	for clientId, multicaster := range bMulticasters {
		scMulticaster, err := DecorateSCMulticast(multicaster)

		if err != nil {
			t.Fatalf("Error while initializing multicaster: %s", err)
		}
		scMulticasters[clientId] = scMulticaster
	}

	senders := selectSender(singleSender, scMulticasters)

	for _, sender := range senders {
		sender := sender
		for i := 0; i < messageToSent; i++ {
			go func() {
				b := make([]byte, 8)
				seqNumber := atomic.AddUint64(&counter, 1)
				binary.LittleEndian.PutUint64(b, seqNumber)
				err := sender.SendToAll(communication.NewMessage(b))
				if err != nil {
					errCh <- err
				}
			}()
		}
	}

	var wg sync.WaitGroup
	wgChan := make(chan bool)
	wg.Add(numMembers)

	sequences := make([][]uint64, 0)

	for _, ch := range chDeliverers {

		ch := ch
		go func() {
			sequence := make([]uint64, 0)

			for completed := make(map[uint64]bool); len(completed) < len(senders)*messageToSent; {
				select {
				case message := <-ch:

					sequenceNumber := binary.LittleEndian.Uint64(message.Payload)

					sequence = append(sequence, sequenceNumber)
					completed[sequenceNumber] = true

				case <-time.After(time.Second * 60):
					errCh <- errors.New("timeoutExceeded")
					return
				}
			}

			sequences = append(sequences, sequence)

			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		wgChan <- true
		close(wgChan)
	}()

	select {
	case <-wgChan:
		break
	case err := <-errCh:
		t.Fatalf("Error while multicasting: %s", err)
	}

	for i := 0; i < len(senders)*messageToSent; i++ {
		var seq *uint64
		seq = nil
		for j := 0; j < numMembers; j++ {
			if seq == nil {
				seq = &sequences[j][i]
			} else if *seq != sequences[j][i] {
				t.Fatalf("Wrong sequence - Expected: %d, Actual: %d", *seq, sequences[j][i])
			}
		}

	}
}
