// Package multicast defines interface for multicast operations and it implements multicast algorithms
package multicast

import (
	"encoding/binary"
	"errors"
	"fmt"
	"gitlab.com/f_mancini/distributedMulticast/internal/deliverer"
	"gitlab.com/f_mancini/distributedMulticast/internal/localsender"
	"gitlab.com/f_mancini/distributedMulticast/internal/randstring"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBmulticast_Init(t *testing.T) {

	mtId := "testBMulticast"
	clientId := "testId"
	numTargets := 2

	// Initializing bMulticast
	ch := make(chan communication.Message)
	smplDlv := deliverer.NewChanDeliverer(ch)

	members := make([]*E2EMember, 0)

	for i := 0; i < numTargets; i++ {

		target := &E2EMember{
			Id:      fmt.Sprintf("qwerty-%d", i),
			Address: "127.0.0.1",
			Sender:  localsender.NewWithDelay(deliverer.NewChanDeliverer(make(chan communication.Message)), 2000),
		}

		members = append(members, target)
	}
	bm := new(bMulticast)

	bm.Init(mtId, clientId, smplDlv, members)

	// Checking if bMulticast is correctly initialized
	if mtId != bm.multicastId {
		t.Fatalf("Multicast ID mismatch - Expected: %s, Actual: %s", mtId, bm.multicastId)
	}

	if clientId != bm.clientId {
		t.Fatalf("Client ID mismatch - Expected: %s, Actual: %s", clientId, bm.clientId)
	}

	if smplDlv != bm.deliverer {
		t.Fatal("Deliverer mismatch")
	}

	for index, expectedMember := range members {
		actualMember := bm.members[index]
		if expectedMember.Id != actualMember.Id || expectedMember.Address != actualMember.Address || expectedMember.Sender != actualMember.Sender {
			t.Fatalf("E2EMember mismatch - Expected: {Id: %s, Address: %s}, Actual: {Id: %s, Address: %s}",
				expectedMember.Id, expectedMember.Address, actualMember.Id, actualMember.Address)

		}

	}
}

func TestBmulticast_SendToAll(t *testing.T) {
	mtId := "testBMulticast"
	clientId := "testId"
	messageString := randstring.RandSeq(10)
	numMembers := 3

	// Initializing bMulticast
	appCh := make(chan communication.Message)
	smplDlv := deliverer.NewChanDeliverer(appCh)

	members := make([]*E2EMember, 0)

	outputCh := make([]chan communication.Message, 0)

	for i := 0; i < numMembers-1; i++ {
		ch := make(chan communication.Message)
		target := &E2EMember{
			Id:      fmt.Sprintf("qwerty%d", i),
			Address: "127.0.0.1",
			Sender:  localsender.NewWithDelay(deliverer.NewChanDeliverer(ch), 2000),
		}

		outputCh = append(outputCh, ch)

		members = append(members, target)
	}

	bm := new(bMulticast)

	bm.Init(mtId, clientId, smplDlv, members)

	errCh := make(chan error)

	var wg sync.WaitGroup
	wgChan := make(chan bool)
	wg.Add(numMembers + 1)

	// Multicasting message
	go func() {
		err := bm.SendToAll(communication.Message{
			Metadata: make(map[string]string),
			Payload:  []byte(messageString),
		})
		if err != nil {
			errCh <- err
			return
		}
		wg.Done()

	}()

	// Checking that bMulticast correctly send message to all members (bMulticast itself, too)
	for _, ch := range outputCh {
		go waitMessage(&wg, ch, messageString, errCh)
	}

	go waitMessage(&wg, appCh, messageString, errCh)

	go func() {
		wg.Wait()
		wgChan <- true
	}()

	select {
	case <-wgChan:
	case err := <-errCh:
		t.Fatalf("Error while receiving messages: %s", err)
	case <-time.After(time.Second * 60):
		t.Fatalf("Missing messages")
	}

}

func waitMessage(wg *sync.WaitGroup, appCh chan communication.Message, messageString string, errCh chan error) {
	func() {
		defer wg.Done()
		message := <-appCh
		if string(message.Payload) != messageString {
			errCh <- errors.New(fmt.Sprintf("wrongMessage: expected: %s - actual: %s", messageString, string(message.Payload)))
		}
	}()
}

func TestBmulticast_Deliver(t *testing.T) {
	mtId := "testBMulticast"
	clientId := "testId"
	messageString := randstring.RandSeq(20)
	numMembers := 3

	// Initializing bMulticast
	ch := make(chan communication.Message)
	smplDlv := deliverer.NewChanDeliverer(ch)

	members := make([]*E2EMember, 0)

	outputCh := make(chan communication.Message)

	for i := 0; i < numMembers-1; i++ {
		target := &E2EMember{
			Id:      fmt.Sprintf("qwerty%d", i),
			Address: "127.0.0.1",
			Sender:  localsender.NewWithDelay(deliverer.NewChanDeliverer(outputCh), 2000),
		}

		members = append(members, target)
	}

	bm := new(bMulticast)

	bm.Init(mtId, clientId, smplDlv, members)

	errCh := make(chan error)

	// Delivering message to bMulticast
	go func() {
		err := bm.Deliver(communication.Message{
			Metadata: make(map[string]string),
			Payload:  []byte(messageString),
		})
		if err != nil {
			errCh <- err
		}
	}()

	// Waiting for message to be delivered to the upper level
	select {
	case message := <-ch:
		if string(message.Payload) != messageString {
			t.Fatalf("wrongMessage: expected: %s - actual: %s", messageString, string(message.Payload))
		}
	case err := <-errCh:
		t.Fatalf("Error while delivering message: %s", err)
	case <-time.After(time.Second * 60):
		t.Fatalf("Message not delivered")
	}
}

func TestBMulticastSingle(t *testing.T) {
	bMulticasting(t, true)
}

func TestBMulticastMultiple(t *testing.T) {
	bMulticasting(t, false)
}

func bMulticasting(t *testing.T, singleSender bool) {
	numMembers := 3
	delay := 1000
	messageToSent := 100

	err := ConfigPool(messageToSent)
	if err != nil {
		t.Fatalf("Error while configuring pool: %s", err)
	}

	counter := uint64(0)

	// Initializing bMulticast
	members, chDeliverers, err := initTestBMulticast(numMembers, delay)

	if err != nil {
		t.Fatalf("Error while initialializing BMultitcast: %s", err)
	}

	errCh := make(chan error)

	// Selecting senders
	senders := selectSender(singleSender, members)

	// Sending messages
	for _, multicaster := range senders {
		multicaster := multicaster

		for i := 0; i < messageToSent; i++ {
			go func() {
				b := make([]byte, 8)
				binary.LittleEndian.PutUint64(b, atomic.AddUint64(&counter, 1))

				err := multicaster.SendToAll(communication.NewMessage(b))
				if err != nil {
					errCh <- err
				}
			}()
		}
	}

	// Checking that all members receive all messages
	var wg sync.WaitGroup
	wgChan := make(chan bool)
	wg.Add(numMembers)
	for _, ch := range chDeliverers {
		ch := ch
		go func() {
			for messages := make(map[uint64]bool); len(messages) < messageToSent*len(senders); {
				select {
				case message := <-ch:
					messages[binary.LittleEndian.Uint64(message.Payload)] = true
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

func selectSender(singleSender bool, members map[string]*Container) map[string]*Container {
	senders := make(map[string]*Container, 0)

	if singleSender {
		selected := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(members))
		index := 0
		for key, member := range members {
			if index == selected {
				senders[key] = member
				break
			}
			index++
		}
	} else {

		for key, elem := range members {
			senders[key] = elem
		}
	}
	return senders
}

func initTestBMulticast(numMembers int, delay int) (map[string]*Container, map[string]chan communication.Message, error) {
	members := make(map[string]*Container, numMembers)
	chDeliverers := make(map[string]chan communication.Message, numMembers)
	appDeliverers := make(map[string]*deliverer.ChanDeliverer, numMembers)
	e2eDelivereres := make(map[string]*deliverer.SimpleDeliverer, numMembers)

	multicastId := randstring.RandSeq(20)

	for i := 0; i < numMembers; i++ {
		clientId := randstring.RandSeq(10)
		for _, ok := e2eDelivereres[clientId]; ok; _, ok = e2eDelivereres[clientId] {
			clientId = randstring.RandSeq(10)
		}

		lD := new(deliverer.SimpleDeliverer)

		e2eDelivereres[clientId] = lD

		outputCh := make(chan communication.Message, 50)

		smplDlv := deliverer.NewChanDeliverer(outputCh)

		chDeliverers[clientId] = outputCh
		appDeliverers[clientId] = smplDlv
	}

	for clientId, lclDlv := range e2eDelivereres {
		group := make([]*E2EMember, 0)
		for cltId, lD := range e2eDelivereres {
			if clientId != cltId {
				member := &E2EMember{
					Id:      cltId,
					Address: cltId,
					Sender:  localsender.NewWithDelay(lD, delay),
				}
				group = append(group, member)
			}
		}

		multicaster, err := NewBMulticast(multicastId, clientId, appDeliverers[clientId], group)

		if err != nil {
			return nil, nil, err
		}

		members[clientId] = multicaster

		err = lclDlv.SetDeliverer(multicaster)
		if err != nil {
			return nil, nil, err
		}
	}
	return members, chDeliverers, nil
}
