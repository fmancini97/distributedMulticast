// Package multicast defines interface for multicast operations and it implements multicast algorithms
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

func TestSeqMulticast_Init(t *testing.T) {

	mtId := "testSeqMulticast"
	clientId := randstring.RandSeq(10)

	numMembers := 3

	// Initializing seqMulticaster
	ch := make(chan communication.Message)
	smplDlv := deliverer.NewChanDeliverer(ch)

	mtChan := make(chan communication.Message)

	members := make([]string, 0)
	membersMap := make(map[string]bool)

	members = append(members, clientId)
	membersMap[clientId] = true

	for i := 0; i < numMembers-1; i++ {
		memberId := randstring.RandSeq(10)
		for _, ok := membersMap[memberId]; ok; {
			memberId = randstring.RandSeq(10)
		}

		members = append(members, memberId)
		membersMap[memberId] = true
	}

	fakeMt := new(fakeMulticast)
	fakeMt.mtId = mtId
	fakeMt.clientId = clientId
	fakeMt.group = members
	fakeMt.deliverer = smplDlv
	fakeMt.mtChan = mtChan

	seqm := new(seqMulticast)

	err := seqm.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing multicaster: %s", err)
	}

	// Checking if seqMulticast is correctly initialized
	err = checkSequencer(seqm, mtId, clientId, smplDlv, members)
	if err != nil {
		t.Fatalf("Error while checking sequecer: %s", err)
	}
}

func checkSequencer(seqm *seqMulticast, mtId string, clientId string, smplDlv *deliverer.ChanDeliverer, members []string) error {
	if mtId != seqm.multicastId {
		return errors.New(fmt.Sprintf("Multicast ID mismatch - Expected: %s, Actual: %s", mtId, seqm.multicastId))
	}

	if clientId != seqm.clientId {
		return errors.New(fmt.Sprintf("Client ID mismatch - Expected: %s, Actual: %s", clientId, seqm.clientId))
	}

	if smplDlv != seqm.deliverer {
		return errors.New(fmt.Sprintf("Deliverer mismatch"))
	}

	for index, expectedMember := range members {
		if seqm.group[index] != expectedMember {
			return errors.New(fmt.Sprintf("Missing member: %s", expectedMember))
		}
	}
	return nil
}

func TestSeqMulticastMaster_Init(t *testing.T) {

	mtId := "testSeqMulticastMaster"
	clientId := randstring.RandSeq(10)

	numMembers := 3

	// Initializing seqMulticaster
	ch := make(chan communication.Message)
	smplDlv := deliverer.NewChanDeliverer(ch)

	mtChan := make(chan communication.Message)

	members := make([]string, 0)
	membersMap := make(map[string]bool)

	members = append(members, clientId)
	membersMap[clientId] = true

	for i := 0; i < numMembers-1; i++ {
		memberId := randstring.RandSeq(10)
		for _, ok := membersMap[memberId]; ok; {
			memberId = randstring.RandSeq(10)
		}

		members = append(members, memberId)
		membersMap[memberId] = true
	}

	fakeMt := new(fakeMulticast)
	fakeMt.mtId = mtId
	fakeMt.clientId = clientId
	fakeMt.group = members
	fakeMt.deliverer = smplDlv
	fakeMt.mtChan = mtChan

	seqm := new(seqMulticastMaster)

	err := seqm.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing multicaster: %s", err)
	}

	// Checking if seqMulticast is correctly initialized
	err = checkSequencer(seqm.seqmt, mtId, clientId, smplDlv, members)
	if err != nil {
		t.Fatalf("Error while checking sequecer: %s", err)
	}
}

func TestSeqMulticast_SendToAll(t *testing.T) {
	mtId := "testSeqMulticast"
	clientId := randstring.RandSeq(10)

	numMessages := uint64(3)
	numMembers := 3

	// Initializing seqMulticast
	ch := make(chan communication.Message)
	smplDlv := deliverer.NewChanDeliverer(ch)

	mtChan := make(chan communication.Message)

	members := make([]string, 0)
	membersMap := make(map[string]bool)

	members = append(members, clientId)
	membersMap[clientId] = true

	for i := 0; i < numMembers-1; i++ {
		memberId := randstring.RandSeq(10)
		for _, ok := membersMap[memberId]; ok; {
			memberId = randstring.RandSeq(10)
		}

		members = append(members, memberId)
		membersMap[memberId] = true
	}

	fakeMt := new(fakeMulticast)
	fakeMt.mtId = mtId
	fakeMt.clientId = clientId
	fakeMt.group = members
	fakeMt.deliverer = smplDlv
	fakeMt.mtChan = mtChan

	seqm := new(seqMulticast)
	err := seqm.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing Sequencer: %s", err)
	}

	// Multicasting message
	seqMulticastSendToAll(t, numMessages, seqm, mtChan)
}

func TestSeqMulticastMaster_SendToAll(t *testing.T) {
	mtId := "testSeqMulticast"
	clientId := randstring.RandSeq(10)

	numMessages := uint64(3)
	numMembers := 3

	// Initializing seqMulticast
	ch := make(chan communication.Message)
	smplDlv := deliverer.NewChanDeliverer(ch)

	mtChan := make(chan communication.Message)

	members := make([]string, 0)
	membersMap := make(map[string]bool)

	members = append(members, clientId)
	membersMap[clientId] = true

	for i := 0; i < numMembers-1; i++ {
		memberId := randstring.RandSeq(10)
		for _, ok := membersMap[memberId]; ok; {
			memberId = randstring.RandSeq(10)
		}

		members = append(members, memberId)
		membersMap[memberId] = true
	}

	fakeMt := new(fakeMulticast)
	fakeMt.mtId = mtId
	fakeMt.clientId = clientId
	fakeMt.group = members
	fakeMt.deliverer = smplDlv
	fakeMt.mtChan = mtChan

	seqm := new(seqMulticastMaster)
	err := seqm.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing Sequencer: %s", err)
	}

	// Multicasting message
	seqMulticastSendToAll(t, numMessages, seqm, mtChan)
}

func seqMulticastSendToAll(t *testing.T, numMessages uint64, seqm Multicast, mtChan chan communication.Message) {
	errCh := make(chan error)

	// Multicasting messages
	for counter := uint64(0); counter < numMessages; counter++ {
		counter := counter
		go func() {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, counter+1)
			err := seqm.SendToAll(communication.NewMessage(b))
			if err != nil {
				errCh <- err
			}
		}()
	}

	// Receiving messages
	for receivedMsgs := make(map[string]bool); uint64(len(receivedMsgs)) == numMessages; {
		select {
		case message := <-mtChan:
			// Checking that message is correct
			err := checkSeqDataMessage(message)
			if err != nil {
				t.Fatalf("Error while checking message: %s", err)
			}
			messageId, _ := message.Metadata[fmt.Sprintf("%s:messageId", seqHeader)]
			receivedMsgs[messageId] = true
		case err := <-errCh:
			t.Fatalf("Error while multicasting message: %s", err)
		case <-time.After(time.Second * 60):
			t.Fatalf("Missing message")
		}
	}
}

func checkSeqDataMessage(message communication.Message) error {
	_, ok := message.Metadata[fmt.Sprintf("%s:messageId", seqHeader)]

	if !ok {
		return errors.New("missing messageId")
	}

	messageType, ok := message.Metadata[fmt.Sprintf("%s:messageType", seqHeader)]

	if !ok {
		return errors.New("missing messageId")
	}

	if seqMessageTypeValue[messageType] != seqData {
		return errors.New(fmt.Sprintf("wrong message type - expected: %s, actual: %s", seqMessageTypeName[seqData], messageType))
	}

	return nil
}

func TestSeqMulticast_Deliver(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	mtId := "testSeqMulticast"
	clientId := randstring.RandSeq(10)

	numMessages := uint64(6)
	numMembers := 3

	// Initializing seqMulticast
	ch := make(chan communication.Message)
	smplDlv := deliverer.NewChanDeliverer(ch)

	mtChan := make(chan communication.Message)

	members := make([]string, 0)
	membersMap := make(map[string]bool)

	members = append(members, clientId)
	membersMap[clientId] = true

	for i := 0; i < numMembers-1; i++ {
		memberId := randstring.RandSeq(10)
		for _, ok := membersMap[memberId]; ok; {
			memberId = randstring.RandSeq(10)
		}

		members = append(members, memberId)
		membersMap[memberId] = true
	}

	fakeMt := new(fakeMulticast)
	fakeMt.mtId = mtId
	fakeMt.clientId = clientId
	fakeMt.group = members
	fakeMt.deliverer = smplDlv
	fakeMt.mtChan = mtChan

	seqm := new(seqMulticast)
	err := seqm.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing sequencer: %s", err)
	}

	errCh := make(chan error)

	messages := make([]communication.Message, 0)

	// Preparing messages to be delivered
	for i := uint64(0); i < numMessages; i++ {
		metadata := make(map[string]string)

		messageId := fmt.Sprintf("%s:%d", seqm.group[r.Intn(len(seqm.group))], i)

		metadata[fmt.Sprintf("%s:messageId", seqHeader)] = messageId
		metadata[fmt.Sprintf("%s:messageType", seqHeader)] = seqMessageTypeName[seqData]

		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, i+1)

		messages = append(messages, communication.Message{
			Metadata: metadata,
			Payload:  b,
		})

		orderMetadata := make(map[string]string)
		orderMetadata[fmt.Sprintf("%s:messageId", seqHeader)] = messageId
		orderMetadata[fmt.Sprintf("%s:messageType", seqHeader)] = seqMessageTypeName[seqOrder]
		orderMetadata[fmt.Sprintf("%s:seqOrder", seqHeader)] = strconv.FormatUint(i+1, 10)

		messages = append(messages, communication.Message{
			Metadata: orderMetadata,
			Payload:  nil,
		})
	}

	// Delivering messages
	for _, index := range r.Perm(len(messages)) {
		index := index
		go func() {
			err := seqm.Deliver(messages[index])
			if err != nil {
				errCh <- err
			}
		}()
	}

	// Receiving messages
	for i := uint64(0); i < numMessages; i++ {
		expectedCounter := i + 1

		select {
		case message := <-ch:

			// Checking that the message is correct
			err := checkSeqDataMessage(message)
			if err != nil {
				t.Fatalf("Error while checking message: %s", err)
			}

			actualCounter := binary.LittleEndian.Uint64(message.Payload)

			if expectedCounter != actualCounter {
				t.Fatalf("Wrong message delivery - Expected: %d, Actual: %d", expectedCounter, actualCounter)
			}
		case err := <-errCh:
			t.Fatalf("Error while delivering message: %s", err)
		case <-time.After(time.Second * 60):
			t.Fatalf("Missing message")
		}
	}

}

func TestSeqMulticastMaster_Deliver(t *testing.T) {
	mtId := "testSeqMulticast"
	clientId := randstring.RandSeq(10)

	numMessages := uint64(6)
	numMembers := 3

	// Initializing seqMulticast
	ch := make(chan communication.Message)
	smplDlv := deliverer.NewChanDeliverer(ch)

	mtChan := make(chan communication.Message)

	members := make([]string, 0)
	membersMap := make(map[string]bool)

	members = append(members, clientId)
	membersMap[clientId] = true

	for i := 0; i < numMembers-1; i++ {
		memberId := randstring.RandSeq(10)
		for _, ok := membersMap[memberId]; ok; {
			memberId = randstring.RandSeq(10)
		}

		members = append(members, memberId)
		membersMap[memberId] = true
	}

	fakeMt := new(fakeMulticast)
	fakeMt.mtId = mtId
	fakeMt.clientId = clientId
	fakeMt.group = members
	fakeMt.deliverer = smplDlv
	fakeMt.mtChan = mtChan

	seqm := new(seqMulticastMaster)
	err := seqm.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing sequencer: %s", err)
	}

	errCh := make(chan error)

	// Delivering messages
	go func() {
		for i := uint64(0); i < numMessages; i++ {
			metadata := make(map[string]string)

			messageId := fmt.Sprintf("%s:%d", seqm.seqmt.group[rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(seqm.seqmt.group))], i)

			metadata[fmt.Sprintf("%s:messageId", seqHeader)] = messageId
			metadata[fmt.Sprintf("%s:messageType", seqHeader)] = seqMessageTypeName[seqData]

			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, i+1)
			err := seqm.Deliver(communication.Message{
				Metadata: metadata,
				Payload:  b,
			})
			if err != nil {
				errCh <- err
			}
		}
	}()

	// Receiving messages
	for i := uint64(0); i < numMessages; {
		expectedCounter := i + 1

		select {
		case message := <-ch:

			// Checking that message is correct
			err := checkSeqDataMessage(message)
			if err != nil {
				t.Fatalf("Error while checking message: %s", err)
			}

			actualCounter := binary.LittleEndian.Uint64(message.Payload)

			if expectedCounter != actualCounter {
				t.Fatalf("Wrong message delivery - Expected: %d, Actual: %d", expectedCounter, actualCounter)
			}
			i++

		case orderMessage := <-mtChan:
			go func() {
				// Delivering order messages
				err := seqm.Deliver(orderMessage)
				if err != nil {
					errCh <- err
				}
			}()

		case err := <-errCh:
			t.Fatalf("Error while delivering message: %s", err)
		case <-time.After(time.Second * 60):
			t.Fatalf("Missing message")
		}
	}

}

func TestSeqMulticastSingle(t *testing.T) {
	seqMulticaster(t, true)
}

func TestSeqMulticastMultiple(t *testing.T) {
	seqMulticaster(t, false)
}

func seqMulticaster(t *testing.T, singleSender bool) {
	numMembers := 3
	delay := 1000
	messageToSent := 100

	err := ConfigPool(messageToSent)
	if err != nil {
		t.Fatalf("Error while configuring pool: %s", err)
	}

	// Initializing seqMulticast
	bMulticasters, chDeliverers, err := initTestBMulticast(numMembers, delay)

	if err != nil {
		t.Fatalf("Error while initialializing BMultitcast: %s", err)
	}

	errCh := make(chan error)

	masterId := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(bMulticasters))

	index := 0

	counter := uint64(0)

	seqMulticasters := make(map[string]*Container)

	for clientId, multicaster := range bMulticasters {
		var seqmulticaster *Container

		if masterId == index {

			seqmulticaster, err = DecorateSeqMulticast(multicaster, true)

			if err != nil {
				t.Fatalf("Error while initializing sequencer master: %s", err)
			}

		} else {
			seqmulticaster, err = DecorateSeqMulticast(multicaster, false)

			if err != nil {
				t.Fatalf("Error while initializing sequencer worker: %s", err)
			}
		}
		seqMulticasters[clientId] = seqmulticaster
		index++
	}

	senders := selectSender(singleSender, seqMulticasters)

	// Multicasting messages
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
	sequencesMu := new(sync.Mutex)

	// Receiving messages
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
			sequencesMu.Lock()
			sequences = append(sequences, sequence)
			sequencesMu.Unlock()

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

	// Checking that all node have received message in the same order
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
