package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/internal/serverinitializer"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/distribuitedmulticast"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicastregistry"
	"google.golang.org/grpc"
	"os"
	"strconv"
)

func main() {

	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
	logger := log.WithField("app_name", "distributed_multicast")

	if len(os.Args) < 3 {
		logger.Infof("Usage: %s restPort grpcPort [registryAddr]", os.Args[0])
		return
	}

	// Parsing parameters

	restPortString := os.Args[1]

	restPort, err := strconv.ParseUint(restPortString, 10, 32)

	if err != nil {
		logger.WithField("error", err).Fatal("Error while parsing restPort")
	}

	grpcPortString := os.Args[2]

	grpcPort, err := strconv.ParseUint(grpcPortString, 10, 32)

	if err != nil {
		logger.WithField("error", err).Fatal("Error while parsing grpcPort")
	}

	services := []func(registrar grpc.ServiceRegistrar) error{e2e.Register}

	var registryAddr string

	logger.Info("Initializing gRPC server")

	if len(os.Args) == 4 {
		registryAddr = os.Args[3]
	} else {
		logger.Info("Initializing multicast registry")
		services = append(services, multicastregistry.Register)
		registryAddr = fmt.Sprintf(":%d", grpcPort)
	}

	err = serverinitializer.InitializeServer(fmt.Sprintf(":%d", grpcPort), services...)
	if err != nil {
		logger.WithField("error", err).Fatal("Error while initializing server")
	}

	logger.Info("Connecting to Multicast registry")

	err = distribuitedmulticast.Initialize(grpcPort, registryAddr)

	if err != nil {
		logger.WithField("error", err).Fatal("Error while connecting to registry")
	}

	router := gin.Default()

	routerGroup := router.Group("/api/v1")

	routerMap, err := distribuitedmulticast.GetRouterMethods(routerGroup)

	for path, methods := range distribuitedmulticast.GetRestAPI() {
		for methodType, method := range methods {
			routerMethod, ok := routerMap[methodType]

			if !ok {
				logger.Fatalf("Missing routing method: %s", distribuitedmulticast.HttpMethodName[methodType])
			}

			routerMethod(path, method)

		}
	}

	err = router.Run(fmt.Sprintf(":%d", restPort))
	if err != nil {
		log.WithField("error", err).Fatal("Error while starting http server")
	}
}
