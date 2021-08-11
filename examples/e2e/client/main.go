package main

import (
	"bufio"
	"fmt"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e"
	"log"
	"os"
)

func main() {
	// Set up a connection to the server.
	conn, err := e2e.ConnectWithDelay("localhost:8080", 5)

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

		err := conn.Send("deadbeef", communication.Message{
			Metadata: header,
			Payload:  []byte(text),
		})
		if err != nil {
			log.Fatalf("could not send message: %v", err)
		}

	}
}
