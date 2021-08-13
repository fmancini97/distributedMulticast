package utils

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicast"
	"os"
)

func StdinSend(mtId string, multicaster multicast.Multicast, logger *log.Entry) {
	header := make(map[string]string)
	header["ID"] = mtId
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter text: ")
		text, _ := reader.ReadString('\n')

		go func() {
			logger.WithField("message", text).Info("Multicasting message")
			err := multicaster.SendToAll(communication.NewMessage([]byte(text)))
			if err != nil {
				logger.WithField("error", err).Fatal("Failed to add deliverer")
			}
		}()
	}
}
