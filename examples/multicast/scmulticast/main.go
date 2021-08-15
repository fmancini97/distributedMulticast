package main

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/simpledeliverer"
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
	//log.SetLevel(log.DebugLevel)

	logger := log.WithField("app", "example/secmulticast")

	if len(os.Args) < 3 {
		logger.Warnf("Usage: %s programAddress mtId address1 address2...", os.Args[0])
		return
	}

	// Taking client port
	programAddress := os.Args[1]

	err := utils.InitializeServer(programAddress)

	if err != nil {
		logger.WithField("error", err).Fatalf("Failed to initialize server")
	}

	mtId := os.Args[2]

	logger.Info("Initializing multicast...")

	addresses := os.Args[3:]

	connections, err := utils.InitializeConnections(addresses, 2000)

	if err != nil {
		logger.WithField("error", err).Fatalf("Failed to initialize connections")
	}

	msgCh := make(chan communication.Message)

	deliverer := simpledeliverer.New(msgCh)

	multicaster, err := multicast.NewBMulticast(mtId, programAddress, deliverer, connections)

	if err != nil {
		logger.WithField("error", err).Fatalf("Error in creating multicaster")
	}

	multicaster, err = multicast.NewSCMulticast(multicaster)

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
