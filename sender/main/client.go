package main

import (
	"bufio"
	"fmt"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/sender"
	"log"
	"os"
)

func main() {
	// Set up a connection to the server.
	conn, err := sender.ConnectWithDelay("localhost:8080", 5)

	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}

	reader := bufio.NewReader(os.Stdin)
	header := make(map[string]string)
	header["ID"] = "deadbeef"
	for {

		fmt.Print("Enter text: ")
		text, _ := reader.ReadString('\n')
		//fmt.Println(text)

		err := conn.Send("deadbeef", distribuitedOrderedMulticast.Message{
			Metadata: header,
			Payload:  []byte(text),
		})
		if err != nil {
			log.Fatalf("could not send message: %v", err)
		}

	}
}
