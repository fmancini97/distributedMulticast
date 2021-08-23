// Package multicast defines interface for multicast operations and it implements multicast algorithms
package multicast

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"gitlab.com/f_mancini/distributedMulticast/internal/deliverer"
	"gitlab.com/f_mancini/distributedMulticast/internal/randstring"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestVcMulticast_Init(t *testing.T) {

	mtId := "testVcMulticast"
	clientId := randstring.RandSeq(10)

	numMembers := 3

	// Initializing vcMulticast
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

	vcm := new(vcMulticast)

	err := vcm.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing multicaster: %s", err)
	}

	// Checking that vcMulticast is correctly initialized
	err = checkVcMulticast(vcm, mtId, clientId, smplDlv, members)

	if err != nil {
		t.Fatalf("Error while checking vc multicaster: %s", err)
	}
}

func checkVcMulticast(vcm *vcMulticast, mtId string, clientId string, smplDlv communication.Deliverer, members []string) error {
	if mtId != vcm.multicastId {
		return errors.New(fmt.Sprintf("Multicast ID mismatch - Expected: %s, Actual: %s", mtId, vcm.multicastId))
	}

	if clientId != vcm.clientId {
		return errors.New(fmt.Sprintf("Client ID mismatch - Expected: %s, Actual: %s", clientId, vcm.clientId))
	}

	if smplDlv != vcm.deliverer {
		return errors.New("deliverer mismatch")
	}

	for _, expectedMember := range members {
		if _, ok := vcm.group[expectedMember]; !ok {
			return errors.New(fmt.Sprintf("Missing member: %s", expectedMember))
		}
	}

	return nil
}

func TestVcMulticast_SendToAll(t *testing.T) {
	mtId := "testVcMulticast"
	clientId := randstring.RandSeq(10)

	numMessages := uint64(7)
	numMembers := 3

	// Initializing vcMulticast
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

	vcm := new(vcMulticast)
	err := vcm.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing vc multicast: %s", err)
	}

	errCh := make(chan error)

	// Multicasting messages
	for counter := uint64(0); counter < numMessages; counter++ {
		counter := counter
		go func() {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, counter+1)
			err := vcm.SendToAll(communication.NewMessage(b))
			if err != nil {
				errCh <- err
			}
		}()
	}

	recvTimestamps := make(map[uint64]bool)
	recvMessages := make(map[uint64]bool)

	// Receiving messages
	for uint64(len(recvMessages)) == numMessages {
		select {
		case message := <-mtChan:

			// Checking that message is correct
			timestampB64, ok := message.Metadata[fmt.Sprintf("%s:timestamp", vcHeader)]

			if !ok {
				t.Fatal("Missing timestamp")
			}

			timestampBytes, err := base64.StdEncoding.DecodeString(timestampB64)

			if err != nil {
				t.Fatalf("Error while parsing timestamp: %s", err)
			}

			var timestampVector map[string]uint64

			timestampBuffer := bytes.Buffer{}
			timestampBuffer.Write(timestampBytes)
			decoder := gob.NewDecoder(&timestampBuffer)
			err = decoder.Decode(&timestampVector)

			if err != nil {
				t.Fatalf("Error while deconding timestamp: %s", err)

			}

			for clientId, timestampValue := range timestampVector {
				if clientId != vcm.clientId && timestampValue != 0 {
					t.Fatalf("Wrong timestamp - Expected: 0 , Actual: %d", timestampValue)
				}
			}

			messageCounter := binary.LittleEndian.Uint64(message.Payload)

			recvTimestamps[timestampVector[vcm.clientId]] = true
			recvMessages[messageCounter] = true

		case err := <-errCh:
			t.Fatalf("Error while multicasting message: %s", err)
		case <-time.After(time.Second * 60):
			t.Fatalf("Missing message")

		}

		// Checking that all message have been sent
		for i := uint64(0); i < numMessages; i++ {
			_, ok := recvTimestamps[i]
			if !ok {
				t.Fatalf("Missing message with timestamp: %d", i)
			}

			_, ok = recvMessages[i]
			if !ok {
				t.Fatalf("Missing message with payload: %d", i)
			}

		}
	}
}

func TestVcMulticast_Deliver(t *testing.T) {
	mtId := "testVcMulticast"
	clientId := randstring.RandSeq(10)

	numMembers := 5

	// Initializing vcMulticast
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

	vcm := new(vcMulticast)
	err := vcm.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing vc multicast: %s", err)
	}

	// Preparing messages to be sent
	timestampVector := make(map[string]uint64)
	vcTimestampVector := make(map[string]uint64)

	for clientId := range vcm.group {
		timestamp := uint64(rand.Intn(50))
		timestampVector[clientId] = timestamp
		vcTimestampVector[clientId] = timestamp
	}

	vcm.timestamp = vcTimestampVector

	messages := make([]communication.Message, 0)

	firstClientId := members[rand.Intn(len(members))]
	for firstClientId == vcm.clientId {
		firstClientId = members[rand.Intn(len(members))]
	}

	secondClientId := members[rand.Intn(len(members))]

	for secondClientId == firstClientId || secondClientId == vcm.clientId {
		secondClientId = members[rand.Intn(len(members))]
	}

	messageTimestamp := make(map[string]uint64)

	for clientId, timestamp := range timestampVector {
		messageTimestamp[clientId] = uint64(rand.Intn(int(timestamp)))
	}

	messageTimestamp[firstClientId] = timestampVector[firstClientId] + 1

	messageTimestamp[secondClientId] = timestampVector[secondClientId] + 1

	message, err := makeVcMessage(messageTimestamp, firstClientId, uint64(1))

	if err != nil {
		t.Fatalf("Error while making message: %s", err)
	}

	messages = append(messages, *message)

	secondMessageTimestamp := make(map[string]uint64)

	for clientId, timestamp := range timestampVector {
		secondMessageTimestamp[clientId] = uint64(rand.Intn(int(timestamp)))
	}

	secondMessageTimestamp[secondClientId] = timestampVector[secondClientId] + 1

	message, err = makeVcMessage(secondMessageTimestamp, secondClientId, uint64(0))

	if err != nil {
		t.Fatalf("Error while making message: %s", err)
	}

	messages = append(messages, *message)

	errCh := make(chan error)

	// Sending messages
	go func() {
		for _, message := range messages {
			err = vcm.Deliver(message)
			if err != nil {
				errCh <- err
			}
		}
	}()

	// Receiving messages
	for i := uint64(0); i < uint64(len(messages)); i++ {

		select {
		case message := <-ch:

			payload := binary.LittleEndian.Uint64(message.Payload)

			// Checking that delivery order is correct
			if payload != i {
				t.Fatalf("Order mismatch - Expected: %d, Actual: %d", i, payload)
			}

		case err := <-errCh:
			t.Fatalf("Error while delivering message: %s", err)
		case <-time.After(time.Second * 60):
			t.Fatalf("Missing message")
		}
	}

}

func makeVcMessage(messageTimestamp map[string]uint64, clientId string, payload uint64) (*communication.Message, error) {
	timestampBytes := new(bytes.Buffer)

	encoder := gob.NewEncoder(timestampBytes)

	err := encoder.Encode(messageTimestamp)

	if err != nil {
		return nil, err
	}

	timestampB64 := base64.StdEncoding.EncodeToString(timestampBytes.Bytes())

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, payload)
	return &communication.Message{
		Metadata: map[string]string{
			fmt.Sprintf("%s:timestamp", vcHeader): timestampB64,
			fmt.Sprintf("%s:clientId", vcHeader):  clientId,
		},
		Payload: b,
	}, nil
}

type happenedBefore struct {
	before, after uint64
}

func TestVcMulticastSingle(t *testing.T) {
	vcmulticasting(t, true)
}

func TestVcMulticastMultiple(t *testing.T) {
	vcmulticasting(t, false)
}

func vcmulticasting(t *testing.T, singleSender bool) {
	numMembers := 3
	delay := 1000
	messageToSent := uint64(100)

	err := ConfigPool(int(messageToSent))
	if err != nil {
		t.Fatalf("Error while configuring pool: %s", err)
	}

	// Initializing vcMulticast
	bMembers, chDeliverers, err := initTestBMulticast(numMembers, delay)

	if err != nil {
		t.Fatalf("Error while initialializing BMultitcast: %s", err)
	}

	errCh := make(chan error)

	vcMembers := make(map[string]*Container)

	for key, multicaster := range bMembers {
		vcmulticaster, err := DecorateVCMulticast(multicaster)

		if err != nil {
			t.Fatalf("Error while initializing vc multicaster: %s", err)
		}

		vcMembers[key] = vcmulticaster

	}

	senders := selectSender(singleSender, vcMembers)
	sendersCounter := make(map[string]*uint64)
	payloadCounter := uint64(0)
	happenedBefores := make([]happenedBefore, 0)
	happenedBeforeMu := &sync.Mutex{}

	for senderId := range sendersCounter {
		counter := uint64(0)
		sendersCounter[senderId] = &counter
	}

	// Multicasting messages
	for senderId, sender := range senders {
		sender := sender
		counterPointer := new(uint64)
		sendersCounter[senderId] = counterPointer
		go func() {
			for i := atomic.AddUint64(counterPointer, 1); i <= messageToSent; i = atomic.AddUint64(counterPointer, 1) {
				payload := atomic.AddUint64(&payloadCounter, 1)
				time.Sleep(time.Millisecond * 100)
				go func() {
					b := make([]byte, 8)
					binary.LittleEndian.PutUint64(b, payload)
					err := sender.SendToAll(communication.NewMessage(b))
					if err != nil {
						errCh <- err
					}
				}()

			}
		}()
	}

	var wg sync.WaitGroup
	wgChan := make(chan bool)
	wg.Add(numMembers)

	sequences := make([][]uint64, 0)
	sequencesMu := &sync.Mutex{}

	for clientId, ch := range chDeliverers {
		ch := ch
		clientId := clientId
		senderCounter := sendersCounter[clientId]
		_, canSend := senders[clientId]

		go func() {
			sequence := make([]uint64, 0)
			for i := uint64(0); i < uint64(len(senders))*messageToSent; i++ {
				select {
				case message := <-ch:
					payload := binary.LittleEndian.Uint64(message.Payload)
					sequence = append(sequence, payload)

					if canSend && rand.Intn(2) == 1 {
						// Happened-before relation
						counter := atomic.AddUint64(senderCounter, 1)
						if counter <= messageToSent {
							newPayload := atomic.AddUint64(&payloadCounter, 1)
							go func() {
								container := vcMembers[clientId]
								b := make([]byte, 8)
								binary.LittleEndian.PutUint64(b, newPayload)

								err := container.SendToAll(communication.NewMessage(b))
								if err != nil {
									errCh <- err
								}
							}()
							happenedBeforeMu.Lock()
							happenedBefores = append(happenedBefores, happenedBefore{
								before: payload,
								after:  newPayload,
							})
							happenedBeforeMu.Unlock()
						}

					}

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

	// Checking Causal order
	happenedBeforeMu.Lock()
	sequencesMu.Lock()
	for _, hB := range happenedBefores {
		waitCheck := new(sync.WaitGroup)

		waitCheck.Add(numMembers)

		for j := 0; j < numMembers; j++ {
			sequenceToCheck := sequences[j]

			go func() {
				beforeIndex := -1
				afterIndex := -1

				for index, elem := range sequenceToCheck {
					if elem == hB.before {
						beforeIndex = index
					}
					if elem == hB.after {
						afterIndex = index
					}
				}

				if beforeIndex == -1 || afterIndex == -1 {
					errCh <- errors.New(fmt.Sprintf("missing message - beforeMessage: %d, beforeIndex: %d, afterMessage: %d, afterIndex: %d", hB.before, beforeIndex, hB.after, afterIndex))
					return
				}

				if beforeIndex >= afterIndex {
					errCh <- errors.New(fmt.Sprintf("wrong order - beforeMessage: %d, beforeIndex: %d, afterMessage: %d, afterIndex: %d", hB.before, beforeIndex, hB.after, afterIndex))
					return
				}

				waitCheck.Done()
			}()
		}

		waitCheckChan := make(chan bool)

		go func() {
			waitCheck.Wait()
			waitCheckChan <- true
		}()

		select {
		case <-waitCheckChan:
		case err := <-errCh:
			t.Fatalf("Error while checking order: %s", err)
		}

	}

}
