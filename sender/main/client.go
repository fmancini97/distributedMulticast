package main

import (
	"bufio"
	"context"
	"fmt"
	pb "gitlab.com/f_mancini/distribuitedOrderedMulticast/sender/service"
	"google.golang.org/grpc"
	"log"
	"os"
	"time"
)

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {

		}
	}(conn)
	c := pb.NewSenderClient(conn)

	reader := bufio.NewReader(os.Stdin)
	header := make(map[string]string)
	header["ID"] = "deadbeef"
	for {

		fmt.Print("Enter text: ")
		text, _ := reader.ReadString('\n')
		//fmt.Println(text)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		_, err := c.Send(ctx, &pb.Message{Metadata: header, Payload: []byte(text)})
		if err != nil {
			log.Fatalf("could not greet: %v", err)
		}

		cancel()

	}
}
