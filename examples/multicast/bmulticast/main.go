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
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicast"
	"strings"
)

func main() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
	logger := log.WithField("app_name", "example_bmulticast")

	nodePort := flag.Uint("nodePort", 8080, "multicast node port")
	multicastId := flag.String("multicastId", "bmulticastTopic", "multicast identifier")
	membersAddrsString := flag.String("membersAddrs", ":8081,:8082", "addresses of the multicast group participants")
	numThreads := flag.Uint("numThreads", 1, "number of threads used to multicast messages")
	delay := flag.Uint("delay", 0, "delay for sending operations (ms)")
	logLevelString := flag.String("logLevel", "info", fmt.Sprintf("log level %s", utils.GetLogLevels()))

	flag.Parse()

	logLevel, err := log.ParseLevel(*logLevelString)

	if err != nil {
		logger.WithField("error", err).Warn("Error while parsing string")
	} else {
		log.SetLevel(logLevel)
	}

	log.WithFields(log.Fields{
		"logLevel":      *logLevelString,
		"multicastId":   *multicastId,
		"nodePort":      *nodePort,
		"memebersAddrs": *membersAddrsString,
		"delay":         *delay,
		"numThreads":    *numThreads,
	}).Debug("Starting application with the following parameters")

	errCh := make(chan error)

	nodeAddr := fmt.Sprintf(":%d", *nodePort)

	go func() {
		err := serverinitializer.StartServer(nodeAddr, e2e.Register)
		if err != nil {
			errCh <- err
		}
	}()

	logger.Info("Initializing multicast...")

	membersAddrs := strings.Split(*membersAddrsString, ",")

	connections, err := e2e.InitializeConnections(membersAddrs, *delay)

	if err != nil {
		logger.WithField("error", err.Error()).Fatalf("Failed to initialize connections")
	}

	msgCh := make(chan communication.Message)

	chanDlv := deliverer.NewChanDeliverer(msgCh)

	multicaster, err := multicast.NewBMulticast(*multicastId, nodeAddr, chanDlv, connections)

	if err != nil {
		logger.WithField("error", err.Error()).Fatalf("Error in creating multicaster")
	}

	err = e2e.RegisterDeliverer(*multicastId, multicaster)

	if err != nil {
		logger.WithField("error", err.Error()).Fatal("Failed to add deliverer")
	}

	logger.Info("Configuring thread pool")
	err = multicast.ConfigPool(int(*numThreads))
	if err != nil {
		logger.WithField("error", err).Fatal("Error while configuring thread pool")
	}

	logger.Info("Ready to receive message")

	go utils.PrintMessages(msgCh, logger)

	go func() {
		err := utils.StdinMulticast(*multicastId, multicaster, logger)
		if err != nil {
			errCh <- err
		}
	}()

	err = <-errCh

	if err != nil {
		logger.WithField("error", err.Error()).Fatal("Failed while multicasting")
	}
}
