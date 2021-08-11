package main

import (
	"bufio"
	"fmt"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicast"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"strconv"
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
	// Taking client port
	portString := os.Args[1]
	port, err := strconv.Atoi(portString)
	if err != nil {
		log.Fatalf("failed to read port: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	err = e2e.Register(s)
	if err != nil {
		log.Fatalf("failed to register: %v", err)
	}

	go func() {
		log.Printf("server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	log.Printf("Initializing multicast...")

	// Taking other ports
	portsString := os.Args[2:]

	ports := make([]multicast.Target, 0)

	for _, portString = range portsString {
		port, err := strconv.Atoi(portString)
		if err != nil {
			log.Fatalf("failed to read port: %v", err)
		}
		address := fmt.Sprintf("localhost:%d", port)
		grpcSender, err := e2e.ConnectWithDelay(address, 2000)
		if err != nil {
			log.Printf("failed to connect: %v", err)
		}

		ports = append(ports, multicast.Target{Address: address, Sender: grpcSender, Id: address})
	}

	deliverer := consumer{make(chan communication.Message)}

	bmulticaster := new(multicast.Bmulticast)
	bmulticaster.Init("deadbeef", deliverer, ports)

	fifomulticaster := new(multicast.FifoMulticast)
	fifomulticaster.Init("deadbeef", fmt.Sprintf("localhost:%d", port), bmulticaster, ports)

	err = e2e.AddDeliverer("deadbeef", fifomulticaster)

	if err != nil {
		log.Fatalf("failed to add channel: %v", err)
	}

	log.Printf("Ready to receive message")

	go func() {
		log.Printf("Start")
		for {
			select {
			case message := <-deliverer.ch:
				log.Printf("New message: Header: %s - Payload: %s", message, string(message.Payload))
			}
		}
	}()

	reader := bufio.NewReader(os.Stdin)
	header := make(map[string]string)
	header["ID"] = "deadbeef"
	for {

		fmt.Print("Enter text: ")
		text, _ := reader.ReadString('\n')
		//fmt.Println(text)

		go func() {
			err := fifomulticaster.SendToAll(communication.NewMessage([]byte(text)))
			if err != nil {
				log.Fatalf("could not send message: %v", err)
			}
		}()

	}

}
