package utils

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicast"
	"os"
)

func StdinMulticast(mtId string, multicaster multicast.Multicast, logger *log.Entry) error {
	errCh := make(chan error)

	header := make(map[string]string)
	header["ID"] = mtId
	go func() {
		for {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Enter text: ")
			text, _ := reader.ReadString('\n')
			text = text[:len(text)-1]

			go func() {
				logger.WithField("message", text).Info("Multicasting message")
				err := multicaster.SendToAll(communication.NewMessage([]byte(text)))
				if err != nil {
					errCh <- err
				}
				logger.WithField("message", text).Info("Message sent")
			}()
		}
	}()

	err := <-errCh

	return err
}

func StdinSend(streamId string, sender communication.Sender, logger *log.Entry) error {
	errCh := make(chan error)

	go func() {
		for {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Enter text: ")
			text, _ := reader.ReadString('\n')
			text = text[:len(text)-1]

			go func() {
				logger.WithField("message", text).Info("Sending message")
				err := sender.Send(streamId, communication.NewMessage([]byte(text)))
				if err != nil {
					errCh <- err
					return
				}
				logger.WithField("message", text).Info("Message sent")
			}()
		}
	}()

	err := <-errCh

	return err
}
