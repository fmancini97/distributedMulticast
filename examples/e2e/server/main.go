package main

import (
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distributedMulticast/internal/deliverer"
	"gitlab.com/f_mancini/distributedMulticast/internal/serverinitializer"
	"gitlab.com/f_mancini/distributedMulticast/internal/utils"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distributedMulticast/pkg/e2e"
)

func main() {

	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
	logger := log.WithField("app_name", "e2e_server")

	serverPort := flag.Uint("serverPort", 8080, "e2e server port")
	streamId := flag.String("streamId", "e2eStream", "e2e stream identifier")

	logLevelString := flag.String("logLevel", "info", fmt.Sprintf("log level %s", utils.GetLogLevels()))

	flag.Parse()

	logLevel, err := log.ParseLevel(*logLevelString)

	if err != nil {
		logger.WithField("error", err).Warn("Error while parsing string")
	} else {
		log.SetLevel(logLevel)
	}

	log.WithFields(log.Fields{
		"logLevel":   *logLevelString,
		"streamId":   *streamId,
		"serverPort": *serverPort,
	}).Debug("Starting application with the following parameters")

	logger.WithField("serverPort", *serverPort).Info("Starting e2e server")

	errCh := make(chan error)

	go func() {
		err := serverinitializer.StartServer(fmt.Sprintf(":%d", *serverPort), e2e.Register)
		if err != nil {
			errCh <- err
		}
	}()

	messCh := make(chan communication.Message)

	chanDeliverer := deliverer.NewChanDeliverer(messCh)

	err = e2e.RegisterDeliverer(*streamId, chanDeliverer)
	if err != nil {
		logger.WithField("error", err.Error()).Fatal("Failed to add e2e channel")
	}

	for {
		select {
		case message := <-messCh:
			logger.WithFields(log.Fields{"header": message.Metadata, "payload": message.Payload}).Info("New message")

		case err := <-errCh:
			logger.WithField("error", err.Error()).Fatal("Error while running server")
		}
	}
}
