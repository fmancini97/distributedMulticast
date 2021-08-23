package main

import (
	"context"
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distributedMulticast/internal/deliverer"
	"gitlab.com/f_mancini/distributedMulticast/internal/multicastmapper"
	"gitlab.com/f_mancini/distributedMulticast/internal/serverinitializer"
	"gitlab.com/f_mancini/distributedMulticast/internal/utils"
	"gitlab.com/f_mancini/distributedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distributedMulticast/pkg/e2e"
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicast"
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicastregistry"
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicastregistry/registrygrpc"
	"google.golang.org/grpc"
	"sync"
	"time"
)

func main() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
	logger := log.WithField("app_name", "distributed_multicast")

	grpcPort := flag.Uint("grpcPort", 90, "port number of the grpc server")
	appMulticast := flag.Bool("appMulticast", false, "start distributed multicast node")
	registryMulticast := flag.Bool("registryMulticast", false, "start multicast registry")
	registryAddr := flag.String("registryAddr", ":90", "multicast registry address")
	numThreads := flag.Uint("numThreads", 1, "number of threads used to multicast messages")
	multicastId := flag.String("multicastId", "multicastTopic", "identifier of the multicast group")
	delay := flag.Uint("delay", 0, "delay for sending operations (ms)")
	startMulticastGroup := flag.Bool("startMulticastGroup", false, "start multicast group after registering")
	multicastTypeString := flag.String("multicastType", "BMULTICAST", fmt.Sprintf("multicast typology: %s", multicastregistry.MulticastTypes))
	logLevelString := flag.String("logLevel", "info", fmt.Sprintf("log level %s", utils.GetLogLevels()))

	flag.Parse()

	logLevel, err := log.ParseLevel(*logLevelString)

	if err != nil {
		logger.WithField("error", err).Warn("Error while parsing string")
	} else {
		log.SetLevel(logLevel)
	}

	log.WithFields(log.Fields{
		"grpcPort":            *grpcPort,
		"appMulticast":        *appMulticast,
		"registryMulticast":   *registryMulticast,
		"registryAddr":        *registryAddr,
		"logLevel":            *logLevelString,
		"numThreads":          *numThreads,
		"multicastId":         *multicastId,
		"delay":               *delay,
		"startMulticastGroup": *startMulticastGroup,
		"multicastType":       *multicastTypeString,
	}).Debug("Starting application with the following parameters")

	multicastType, ok := multicastregistry.MulticastType[*multicastTypeString]

	if !ok {
		logger.Fatalf("Multicast type %s not supported", *multicastTypeString)
	}

	logger.Info("Initializing gRPC server")

	services := make([]func(registrar grpc.ServiceRegistrar) error, 0)

	if *appMulticast {
		logger.Debug("Adding e2e service to gRPC server")
		services = append(services, e2e.Register)
	}

	if *registryMulticast {
		logger.Debug("Adding multicast registry to gRPC server")
		services = append(services, multicastregistry.Register)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	errCh := make(chan error)

	go func() {
		err := serverinitializer.StartServer(fmt.Sprintf(":%d", *grpcPort), services...)
		if err != nil {
			errCh <- err
		}
		wg.Done()
	}()

	if *appMulticast {

		logger.Info("Starting multicast app")

		logger.Info("Configuring thread pool")
		err := multicast.ConfigPool(int(*numThreads))
		if err != nil {
			logger.WithField("error", err).Fatal("Error while configuring thread pool")
		}

		logger.Info("Connecting to Multicast registry")

		registryClient, err := multicastregistry.Connect(*registryAddr)

		if err != nil {
			logger.WithField("error", err).Fatal("Error while connecting to registry")
			return
		}

		registrationAns, err := registryClient.Register(context.Background(), &registrygrpc.RegistrationInfo{
			MulticastId:   *multicastId,
			MulticastType: multicastType,
			ClientPort:    uint32(*grpcPort),
		})

		if err != nil {
			logger.WithField("error", err).Fatal("Error during registration")
			return
		}

		groupInfo := registrationAns.GroupInfo

		clientId := registrationAns.ClientId

		isMaster := len(registrationAns.GroupInfo.Members) == 1 // The first is master

		logger = logger.WithField("client_id", clientId)

		if *startMulticastGroup {
			logger.Info("Starting multicast group")
			groupInfo, err = registryClient.StartGroup(context.Background(), &registrygrpc.RequestData{MulticastId: *multicastId, ClientId: clientId})
			if err != nil {
				logger.WithField("error", err).Fatal("Error while starting Communication")
				return
			}
		}

		logger.Info("Waiting for group starting")

		for groupInfo.Status == registrygrpc.Status_OPENING {
			time.Sleep(time.Second * 5)
			groupInfo, err = registryClient.GetStatus(context.Background(), &registrygrpc.MulticastId{MulticastId: *multicastId})
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

				connection, err := e2e.InitializeConnection(member.Id, member.Address, *delay)
				if err != nil {
					logger.WithField("error", err).Fatal("Error while initializing connections")
				}
				e2eConnections = append(e2eConnections, connection)
			}
		}

		multicaster, err := multicast.Initializers[mtType](*multicastId, clientId, dlv, e2eConnections)

		if err != nil {
			logger.WithField("error", err).Fatal("Error while initializing multicaster")
		}

		err = e2e.RegisterDeliverer(*multicastId, multicaster)
		if err != nil {
			logger.WithField("error", err).Fatal("Error while initializing multicaster")
		}

		groupInfo, err = registryClient.Ready(context.Background(), &registrygrpc.RequestData{MulticastId: *multicastId, ClientId: clientId})
		if err != nil {
			logger.WithField("error", err).Fatal("Error while starting Communication")
		}

		logger.Info("Waiting other multicast members")

		for groupInfo.Status == registrygrpc.Status_STARTING {
			time.Sleep(time.Second * 5)
			groupInfo, err = registryClient.GetStatus(context.Background(), &registrygrpc.MulticastId{MulticastId: *multicastId})
			if err != nil {
				logger.WithField("error", err).Fatal("Error while getting service status")
			}
		}

		if groupInfo.Status == registrygrpc.Status_CLOSED {
			logger.WithField("group", groupInfo).Fatal("Group closed")
		}

		logger.Info("Group ready to multicast!!!")

		wg.Add(2)

		go func() {
			utils.PrintMessages(msgCh, logger)
			wg.Done()
		}()

		go func() {
			err := utils.StdinMulticast(*multicastId, multicaster, logger)
			if err != nil {
				errCh <- err
			}
			wg.Done()
		}()
	}

	wgChan := make(chan bool)

	go func() {
		wg.Wait()
		wgChan <- true
	}()

	logger.Info("App started")

	select {
	case err := <-errCh:
		logger.WithField("error", err).Fatal("Error while executing program")
	case <-wgChan:
	}
}
