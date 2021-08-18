package main

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/chandeliverer"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/serverinitializer"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/utils"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicast"
	"os"
)

func main() {

	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
	log.SetLevel(log.DebugLevel)

	logger := log.WithField("app", "example/fifomulticast")

	if len(os.Args) < 3 {
		logger.Warnf("Usage: %s programAddress mtId address1 address2...", os.Args[0])
		return
	}

	// Taking client port
	programAddress := os.Args[1]

	err := serverinitializer.InitializeServer(programAddress)

	if err != nil {
		logger.WithField("error", err).Fatalf("Failed to initialize server")
	}

	logger.Info("Initializing multicast...")

	addresses := os.Args[3:]

	connections, err := serverinitializer.InitializeConnections(addresses, 2000)

	if err != nil {
		logger.WithField("error", err).Fatalf("Failed to initialize connections")
	}

	msgCh := make(chan communication.Message)

	deliverer := chandeliverer.New(msgCh)

	mtId := os.Args[2]

	multicaster, err := multicast.NewBMulticast(mtId, programAddress, deliverer, connections)

	if err != nil {
		logger.WithField("error", err).Fatalf("Error in creating multicaster")
	}

	multicaster, err = multicast.NewFifoMulticast(multicaster)

	if err != nil {
		logger.WithField("error", err).Fatalf("Error in creating multicaster")
	}

	err = e2e.AddDeliverer(mtId, multicaster)

	if err != nil {
		logger.WithField("error", err).Fatal("Failed to add deliverer")
	}

	logger.Info("Ready to receive message")

	go utils.PrintMessages(msgCh, logger)

	utils.StdinSend(mtId, multicaster, logger)

}
