package main

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/deliverer"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/multicastmapper"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/serverinitializer"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/utils"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicast"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicastregistry"
	registrygrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicastregistry/grpc"
	"google.golang.org/grpc"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
	logger := log.WithField("app_name", "distributed_multicast")

	if len(os.Args) < 5 {
		multicastTypes := make([]string, 0)

		for multicastType, _ := range multicastregistry.MulticastType {
			multicastTypes = append(multicastTypes, multicastType)
		}

		logger.Infof("Usage: %s multicastId %s clientPort startGroup [registryAddr]", os.Args[0], multicastTypes)
		return
	}

	// Parsing parameters

	multicastId := os.Args[1]

	multicastType, ok := multicastregistry.MulticastType[os.Args[2]]

	if !ok {
		logger.Fatalf("Multicast type %s not supported", os.Args[2])
	}

	clientPortString := os.Args[3]

	clientPort, err := strconv.ParseUint(clientPortString, 10, 32)

	if err != nil {
		logger.WithField("error", err).Fatal("Error while parsing clientPort")
	}

	startMulticastString := strings.ToLower(os.Args[4])
	if startMulticastString != "true" && startMulticastString != "false" {
		logger.WithField("startMulticastString", startMulticastString).Fatal("Unexpected isMaster parameter")
	}

	startMulticast := startMulticastString == "true"

	services := []func(registrar grpc.ServiceRegistrar) error{e2e.Register}

	var registryAddr string

	logger.Info("Initializing gRPC server")

	if len(os.Args) == 6 {
		registryAddr = os.Args[5]
	} else {
		logger.Info("Initializing multicast registry")
		services = append(services, multicastregistry.Register)
		registryAddr = fmt.Sprintf("localhost:%d", clientPort)
	}

	err = serverinitializer.InitializeServer(fmt.Sprintf(":%d", clientPort), services...)
	if err != nil {
		logger.WithField("error", err).Fatal("Error while initializing server")
	}

	logger.Info("Connecting to Multicast registry")

	registryClient, err := multicastregistry.Connect(registryAddr)

	if err != nil {
		logger.WithField("error", err).Fatal("Error while connecting to registry")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)

	registrationAns, err := registryClient.Register(ctx, &registrygrpc.RegistrationInfo{
		GroupId:       multicastId,
		MulticastType: multicastType,
		ClientPort:    clientPort,
	})
	cancel()

	if err != nil {
		logger.WithField("error", err).Fatal("Error during registration")
	}

	groupInfo := registrationAns.GroupInfo

	clientId := registrationAns.ClientId

	isMaster := len(registrationAns.GroupInfo.Members) == 1 // The first is master

	logger = logger.WithField("client_id", clientId)

	if startMulticast {
		logger.Info("Starting multicast group")
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*60)
		groupInfo, err = registryClient.StartGroup(ctx, &registrygrpc.RequestData{GroupId: multicastId, ClientId: clientId})
		cancel()
		if err != nil {
			logger.WithField("error", err).Fatal("Error while starting Communication")
		}
	}

	logger.Info("Waiting for group starting")

	for groupInfo.Status == registrygrpc.Status_OPENING {
		time.Sleep(time.Second * 5)
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*60)
		groupInfo, err = registryClient.GetStatus(ctx, &registrygrpc.GroupId{GroupId: multicastId})
		cancel()
		if err != nil {
			logger.WithField("error", err).Fatal("Error while getting service status")
		}

	}

	if groupInfo.Status == registrygrpc.Status_CLOSED {
		logger.WithField("group", groupInfo).Fatal("Group closed")
	}

	logger.WithField("group", groupInfo).Info("Initializing Multicast")

	mtType, ok := multicasttypemapper.Type[groupInfo.MulticastType][isMaster]

	if !ok {
		logger.Fatalf("Unknown multicast type: %s", groupInfo.MulticastType)
	}

	msgCh := make(chan communication.Message)

	dlv := deliverer.NewChanDeliverer(msgCh)

	e2eConnections := make([]*multicast.E2EMember, 0)

	for memberId, member := range groupInfo.Members {
		if memberId != clientId {
			logger.Infof("Connecting to: %s", member.Address)

			connection, err := e2e.InitializeConnection(member.Id, member.Address, 2000)
			if err != nil {
				logger.WithField("error", err).Fatal("Error while initializing connections")
			}
			e2eConnections = append(e2eConnections, connection)
		}
	}

	multicaster, err := multicast.MulticastInitializers[mtType](multicastId, clientId, dlv, e2eConnections)

	if err != nil {
		logger.WithField("error", err).Fatal("Error while initializing multicaster")
	}

	err = e2e.AddDeliverer(multicastId, multicaster)
	if err != nil {
		logger.WithField("error", err).Fatal("Error while initializing multicaster")
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*60)
	groupInfo, err = registryClient.Ready(ctx, &registrygrpc.RequestData{GroupId: multicastId, ClientId: clientId})
	cancel()
	if err != nil {
		logger.WithField("error", err).Fatal("Error while starting Communication")
	}

	logger.Info("Waiting other multicast members")

	for groupInfo.Status == registrygrpc.Status_STARTING {
		time.Sleep(time.Second * 5)
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*60)
		groupInfo, err = registryClient.GetStatus(ctx, &registrygrpc.GroupId{GroupId: multicastId})
		cancel()
		if err != nil {
			logger.WithField("error", err).Fatal("Error while getting service status")
		}
	}

	if groupInfo.Status == registrygrpc.Status_CLOSED {
		logger.WithField("group", groupInfo).Fatal("Group closed")
	}

	logger.Info("Group ready to multicast!!!")

	go utils.PrintMessages(msgCh, logger)

	utils.StdinSend(multicastId, multicaster, logger)

}
