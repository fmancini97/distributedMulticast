package utils

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
)

func PrintMessages(msgCh chan communication.Message, logger *log.Entry) {
	for {
		select {
		case message := <-msgCh:
			logger.WithFields(log.Fields{"header": message.Metadata, "payload": string(message.Payload)}).Info("New message")
		}
	}
}
