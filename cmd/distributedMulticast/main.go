package main

import (
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distributedMulticast/internal/envparser"
	"gitlab.com/f_mancini/distributedMulticast/internal/serverinitializer"
	"gitlab.com/f_mancini/distributedMulticast/internal/utils"
	"gitlab.com/f_mancini/distributedMulticast/pkg/distribuitedmulticast"
	"gitlab.com/f_mancini/distributedMulticast/pkg/e2e"
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicastregistry"
	"google.golang.org/grpc"
	"sync"
)

func main() {

	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
	logger := log.WithField("app_name", "distributed_multicast")

	restPort := flag.Uint("restPort", envparser.LookupEnvOrUint("REST_PORT", 80), "port number of the rest server")
	grpcPort := flag.Uint("grpcPort", envparser.LookupEnvOrUint("GRPC_PORT", 90), "port number of the grpc server")
	appMulticast := flag.Bool("appMulticast", envparser.LookupEnvOrBool("APP", false), "start distributed multicast node")
	registryMulticast := flag.Bool("registryMulticast", envparser.LookupEnvOrBool("REGISTRY", false), "start multicast registry")
	registryAddr := flag.String("registryAddr", envparser.LookupEnvOrString("REGISTRY_ADDR", ":90"), "multicast registry address")
	restPath := flag.String("restPath", envparser.LookupEnvOrString("REST_PATH", "/multicast/api/v1"), "path of the rest api")
	numThreads := flag.Uint("numThreads", envparser.LookupEnvOrUint("NUM_THREADS", 1), "number of threads used to multicast messages")
	delay := flag.Uint("delay", envparser.LookupEnvOrUint("DELAY", 0), "delay for sending operations (ms)")
	logLevelString := flag.String("logLevel", envparser.LookupEnvOrString("LOG_LEVEL", "info"), fmt.Sprintf("log level: %s", utils.GetLogLevels()))
	debug := flag.Bool("debugMode", envparser.LookupEnvOrBool("DEBUG", false), "execute distributed multicast node in debug mode")

	flag.Parse()

	logLevel, err := log.ParseLevel(*logLevelString)

	if err != nil {
		logger.WithField("error", err).Warn("Error while parsing string")
	} else {
		log.SetLevel(logLevel)
	}

	log.WithFields(log.Fields{
		"restPort":          *restPort,
		"grpcPort":          *grpcPort,
		"appMulticast":      *appMulticast,
		"registryMulticast": *registryMulticast,
		"registryAddr":      *registryAddr,
		"restPath":          *restPath,
		"logLevel":          *logLevelString,
		"debug":             *debug,
	}).Debug("Starting application with the following parameters")

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

		logger.Info("Starting distributed multicast node")

		wg.Add(1)
		go func() {
			err := distribuitedmulticast.Run(*grpcPort, *restPort, *registryAddr, *restPath, *numThreads, *delay, *debug)
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
		logger.WithField("error", err.Error()).Fatal("Error while executing program")
	case <-wgChan:
	}

}
