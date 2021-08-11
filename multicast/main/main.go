package main

import (
	"bufio"
	"fmt"
	dom "gitlab.com/f_mancini/distribuitedOrderedMulticast"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/multicast"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/multicast/bmulticast"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/sender"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"strconv"
)

type consumer struct {
	ch chan dom.Message
}

func (c consumer) Deliver(message dom.Message) error {
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
	err = sender.Register(s)
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
		grpcSender, err := sender.ConnectWithDelay(address, 2000)
		if err != nil {
			log.Printf("failed to connect: %v", err)
		}

		ports = append(ports, multicast.Target{Address: address, Sender: grpcSender})
	}

	deliverer := consumer{make(chan dom.Message)}

	multicaster, err := bmulticast.New("deadbeef", deliverer, ports)

	err = sender.AddDeliverer("deadbeef", multicaster)

	if err != nil {
		log.Fatalf("failed to add channel: %v", err)
	}

	log.Printf("Ready to receive message")

	go func() {
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

		err = multicaster.SendToAll(dom.Message{
			Metadata: header,
			Payload:  []byte(text),
		})

		if err != nil {
			log.Fatalf("could not send message: %v", err)
		}

	}

}
