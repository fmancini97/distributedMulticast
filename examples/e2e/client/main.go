package main

import (
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distributedMulticast/internal/utils"
	"gitlab.com/f_mancini/distributedMulticast/pkg/e2e"
)

func main() {
	// Set up a connection to the server.

	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
	logger := log.WithField("app_name", "e2e_client")

	serverAddr := flag.String("serverAddr", ":8080", "e2e server to send messages")
	streamId := flag.String("streamId", "e2eStream", "e2e stream identifier")

	delay := flag.Uint("delay", 0, "delay for sending operations")
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
		"serverAddr": *serverAddr,
		"delay":      *delay,
	}).Debug("Starting application with the following parameters")

	logger.WithField("server", *serverAddr).Info("Connecting to e2e server")

	conn, err := e2e.ConnectWithDelay(*serverAddr, *delay)

	if err != nil {
		logger.WithField("error", err.Error()).Fatalf("Failed to connect")
	}

	errCh := make(chan error)
	endCh := make(chan bool)

	go func() {
		err := utils.StdinSend(*streamId, conn, logger)
		if err != nil {
			errCh <- err
		}
	}()

	logger.Info("App started")

	select {
	case err := <-errCh:
		logger.WithField("error", err.Error()).Fatal("Error while executing program")
	case <-endCh:
	}
	logger.Info("Exiting")
}
