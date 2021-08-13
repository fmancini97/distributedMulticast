package multicast

import (
	"fmt"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/localsender"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/simpledeliverer"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"testing"
	"time"
)

func TestBmulticast_Init(t *testing.T) {

	mtId := "testBMulticast"
	clientId := "testId"

	ch := make(chan communication.Message)
	smplDlv := simpledeliverer.New(ch)

	members := make([]Target, 0)

	target := Target{
		Id:      "qwerty",
		Address: "127.0.0.1",
		Sender:  localsender.New(simpledeliverer.New(make(chan communication.Message))),
	}

	members = append(members, target)

	bm := new(bmulticast)

	bm.Init(mtId, clientId, smplDlv, members)

	if mtId != bm.mtId {
		t.Fatalf("Multicast ID mismatch - Expected: %s, Actual: %s", mtId, bm.mtId)
	}

	if clientId != bm.clientId {
		t.Fatalf("Client ID mismatch - Expected: %s, Actual: %s", clientId, bm.clientId)
	}

	if smplDlv != bm.deliverer {
		t.Fatal("Deliverer mismatch")
	}

	for index, expectedMember := range members {
		actualMember := bm.targets[index]
		if expectedMember.Id != actualMember.Id || expectedMember.Address != actualMember.Address || expectedMember.Sender != actualMember.Sender {
			t.Fatalf("Target mismatch - Expected: {Id: %s, Address: %s}, Actual: {Id: %s, Address: %s}",
				expectedMember.Id, expectedMember.Address, actualMember.Id, actualMember.Address)

		}

	}
}

func TestBmulticast_SendToAll(t *testing.T) {
	mtId := "testBMulticast"
	clientId := "testId"

	ch := make(chan communication.Message)
	smplDlv := simpledeliverer.New(ch)

	members := make([]Target, 0)

	outputCh := make(chan communication.Message)

	for i := 0; i < 2; i++ {
		target := Target{
			Id:      fmt.Sprintf("qwerty%d", i),
			Address: "127.0.0.1",
			Sender:  localsender.New(simpledeliverer.New(outputCh)),
		}

		members = append(members, target)
	}

	bm := new(bmulticast)

	bm.Init(mtId, clientId, smplDlv, members)

	go func() {
		err := bm.SendToAll(communication.Message{
			Metadata: make(map[string]string),
			Payload:  nil,
		})
		if err != nil {
			t.Errorf("Error while sending message: %s", err)
			return
		}
	}()

	for i := 0; i < len(members)+1; i++ {
		select {
		case <-ch:
		case <-outputCh:
		case <-time.After(time.Second * 60):
			t.Fatalf("Only %d/%d members have received the message", i, len(members)+1)
		}
	}
}

func TestBmulticast_Deliver(t *testing.T) {
	mtId := "testBMulticast"
	clientId := "testId"

	ch := make(chan communication.Message)
	smplDlv := simpledeliverer.New(ch)

	members := make([]Target, 0)

	outputCh := make(chan communication.Message)

	for i := 0; i < 2; i++ {
		target := Target{
			Id:      fmt.Sprintf("qwerty%d", i),
			Address: "127.0.0.1",
			Sender:  localsender.New(simpledeliverer.New(outputCh)),
		}

		members = append(members, target)
	}

	bm := new(bmulticast)

	bm.Init(mtId, clientId, smplDlv, members)

	go func() {
		err := bm.Deliver(communication.Message{
			Metadata: make(map[string]string),
			Payload:  nil,
		})
		if err != nil {
			t.Errorf("Error while delivering message: %s", err)
			return
		}
	}()

	select {
	case <-ch:
	case <-time.After(time.Second * 60):
		t.Fatalf("Message not delivered")
	}
}
