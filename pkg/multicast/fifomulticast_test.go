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
	"testing"
	"time"
)

func TestFifoMulticast_Init(t *testing.T) {

	mtId := "testFifoMulticast"
	clientId := randstring.RandSeq(10)

	numMembers := 3

	// Initializing fifoMulticast
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

	fifom := new(fifoMulticast)

	err := fifom.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing multicaster: %s", err)
	}

	// Checking that fifoMulticast is correctly initialized
	err = checkFifoMulticast(fifom, mtId, clientId, smplDlv, members)

	if err != nil {
		t.Fatalf("Error while checking fifo multicaster: %s", err)
	}
}

func checkFifoMulticast(fifom *fifoMulticast, mtId string, clientId string, smplDlv communication.Deliverer, members []string) error {
	if mtId != fifom.multicastId {
		return errors.New(fmt.Sprintf("Multicast ID mismatch - Expected: %s, Actual: %s", mtId, fifom.multicastId))
	}

	if clientId != fifom.clientId {
		return errors.New(fmt.Sprintf("Client ID mismatch - Expected: %s, Actual: %s", clientId, fifom.clientId))
	}

	if smplDlv != fifom.deliverer {
		return errors.New("deliverer mismatch")
	}

	for _, expectedMember := range members {
		if _, ok := fifom.members[expectedMember]; !ok {
			return errors.New(fmt.Sprintf("Missing member: %s", expectedMember))
		}
	}

	return nil
}

func TestFifoMulticast_SendToAll(t *testing.T) {
	mtId := "testFifoMulticast"
	clientId := randstring.RandSeq(10)

	numMessages := uint64(3)
	numMembers := 3

	// Initializing fifoMulticast
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

	fifom := new(fifoMulticast)
	err := fifom.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing Fifo multicast: %s", err)
	}

	errCh := make(chan error)

	// Sending messages
	go func() {
		for counter := uint64(0); counter < numMessages; counter++ {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, counter+1)
			err := fifom.SendToAll(communication.NewMessage(b))
			if err != nil {
				errCh <- err
			}
		}
	}()

	for receivedMsgs := make(map[uint64]bool); uint64(len(receivedMsgs)) == numMessages; {
		select {
		case message := <-mtChan:

			// Checking correctness of the received message
			err := checkFifoMessage(message, clientId)

			if err != nil {
				t.Fatalf("Error while checking message: %s", err)
			}

			// Checking that the delivery order is correct
			messageCounter := binary.LittleEndian.Uint64(message.Payload)

			counterString := message.Metadata[fmt.Sprintf("%s:counter", fifoHeader)]

			headerCounter, _ := strconv.ParseUint(counterString, 10, 64)

			if messageCounter != headerCounter {
				t.Fatalf("Wrong order - Expected: %d, Actual: %d", messageCounter, headerCounter)
			}

			receivedMsgs[messageCounter] = true
		case err := <-errCh:
			t.Fatalf("Error while multicasting message: %s", err)
		case <-time.After(time.Second * 60):
			t.Fatalf("Missing message")

		}
	}
}

func checkFifoMessage(message communication.Message, expectedClientId string) error {
	actualClientId, ok := message.Metadata[fmt.Sprintf("%s:clientId", fifoHeader)]

	if !ok {
		return errors.New("missing clientId")
	}

	if actualClientId != expectedClientId {
		return errors.New(fmt.Sprintf("client Id mismatch - expected: %s, actual: %s", expectedClientId, actualClientId))
	}

	counterString, ok := message.Metadata[fmt.Sprintf("%s:counter", fifoHeader)]

	if !ok {
		return errors.New("missing counter")
	}

	_, err := strconv.ParseUint(counterString, 10, 64)

	return err
}

func TestFifoMulticast_Deliver(t *testing.T) {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	mtId := "testFifoMulticast"
	clientId := randstring.RandSeq(10)

	numMessages := uint64(3)
	numMembers := 3

	// Initializing fifoMulticast
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

	fifom := new(fifoMulticast)
	err := fifom.Init(fakeMt)
	if err != nil {
		t.Fatalf("Error while initializing Fifo multicast: %s", err)
	}

	sourceClientId := members[r.Intn(numMembers)]

	errCh := make(chan error)

	// Delivering message
	go func() {
		for _, counter := range r.Perm(int(numMessages)) {
			metadata := make(map[string]string)
			metadata[fmt.Sprintf("%s:counter", fifoHeader)] = strconv.FormatUint(uint64(counter+1), 10)
			metadata[fmt.Sprintf("%s:clientId", fifoHeader)] = sourceClientId

			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(counter+1))

			message := communication.Message{
				Metadata: metadata,
				Payload:  b,
			}

			err := fifom.Deliver(message)
			if err != nil {
				errCh <- err
			}
		}
	}()

	// Receiving messages
	for i := uint64(0); i < numMessages; i++ {
		expectedCounter := i + 1

		select {
		case message := <-ch:

			// Checking that message is correct
			err := checkFifoMessage(message, sourceClientId)
			if err != nil {
				t.Fatalf("Error while checking message: %s", err)
			}

			actualCounter := binary.LittleEndian.Uint64(message.Payload)

			counterString := message.Metadata[fmt.Sprintf("%s:counter", fifoHeader)]

			headerCounter, _ := strconv.ParseUint(counterString, 10, 64)

			// Checking order
			if actualCounter != headerCounter {
				t.Fatalf("Wrong order - Expected: %d, Actual: %d", actualCounter, headerCounter)
			}

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

func TestFifoMulticastSingle(t *testing.T) {
	fifomulticasting(t, true)
}

func TestFifoMulticastMultiple(t *testing.T) {
	fifomulticasting(t, false)
}

func fifomulticasting(t *testing.T, singleSender bool) {
	numMembers := 3
	delay := 1000
	messageToSent := 100

	err := ConfigPool(messageToSent)
	if err != nil {
		t.Fatalf("Error while configuring pool: %s", err)
	}

	// Initializing fifoMulticast
	bMembers, chDeliverers, err := initTestBMulticast(numMembers, delay)

	if err != nil {
		t.Fatalf("Error while initialializing BMultitcast: %s", err)
	}

	errCh := make(chan error)

	fifoMembers := make(map[string]*Container)

	for key, multicaster := range bMembers {
		fifomulticaster, err := DecorateFifoMulticast(multicaster)

		if err != nil {
			t.Fatalf("Error while initializing fifo multicaster: %s", err)
		}

		fifoMembers[key] = fifomulticaster

	}

	// Selecting senders
	senders := selectSender(singleSender, fifoMembers)

	// Sending messages
	for _, sender := range senders {
		sender := sender
		for i := 0; i < messageToSent; i++ {
			i := i
			go func() {
				b := make([]byte, 8)
				binary.LittleEndian.PutUint64(b, uint64(i+1))
				err := sender.SendToAll(communication.NewMessage(b))
				if err != nil {
					errCh <- err
				}
			}()
		}
	}

	// Receiving messages
	var wg sync.WaitGroup
	wgChan := make(chan bool)
	wg.Add(numMembers)
	for _, ch := range chDeliverers {
		ch := ch
		go func() {
			counters := make(map[string]uint64)
			for _, member := range senders {
				clientId, err := member.getClientId()
				if err != nil {
					errCh <- err
					return
				}
				counters[clientId] = 0
			}

			for numMembersCompleted := 0; numMembersCompleted < len(senders); {
				select {
				case message := <-ch:
					clientId, ok := message.Metadata[fmt.Sprintf("%s:clientId", fifoHeader)]

					if !ok {
						errCh <- errors.New("missing clientId")
						return
					}

					counter := counters[clientId]
					counterString, ok := message.Metadata[fmt.Sprintf("%s:counter", fifoHeader)]

					if !ok {
						errCh <- errors.New("missing counter")
						return
					}

					actualCounter, err := strconv.ParseUint(counterString, 10, 64)

					if err != nil {
						errCh <- err
						return
					}

					// Checking delivery order
					if counter+1 != actualCounter {
						errCh <- errors.New(fmt.Sprintf("order mismatch - axpected: %d, actual: %d", counter+1, actualCounter))
						return
					}

					counter++
					counters[clientId] = counter

					if counter == uint64(messageToSent) {
						numMembersCompleted++
					}

				case <-time.After(time.Second * 60):
					errCh <- errors.New("timeoutExceeded")
					return
				}
			}

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
}
