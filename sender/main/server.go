package main

import (
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/sender"
	"google.golang.org/grpc"
	"log"
	"net"
)

func main() {
	lis, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	err = sender.Register(s)
	if err != nil {
		log.Fatalf("failed to register: %v", err)
	}

	go func() {
		ch, err := sender.AddCh("deadbeef")
		if err != nil {
			log.Fatalf("failed to add channel: %v", err)
		}

		for {
			select {
			case message := <-ch:
				log.Printf("New message: Header: %s - Payload: %s", message.Metadata, string(message.Payload))
			}
		}
	}()

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
