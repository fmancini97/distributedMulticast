package main

import (
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e"
	"google.golang.org/grpc"
	"log"
	"net"
)

type consumer struct {
	ch chan communication.Message
}

func (c consumer) Deliver(message communication.Message) error {
	log.Printf("Processing new message")
	c.ch <- message
	return nil
}

func main() {
	lis, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	err = e2e.Register(s)
	if err != nil {
		log.Fatalf("failed to register: %v", err)
	}

	go func() {
		deliverer := consumer{make(chan communication.Message)}
		err := e2e.AddDeliverer("deadbeef", deliverer)
		if err != nil {
			log.Fatalf("failed to add channel: %v", err)
		}

		for {
			select {
			case message := <-deliverer.ch:
				log.Printf("New message: Header: %s - Payload: %s", message, string(message.Payload))
			}
		}
	}()

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
