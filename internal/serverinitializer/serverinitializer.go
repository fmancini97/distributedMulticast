package serverinitializer

import (
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
)

func InitializeServer(programAddress string, grpcServices ...func(grpc.ServiceRegistrar) error) error {
	logger := log.WithField("address", programAddress)
	logger.Debug("Initializing server")

	lis, err := net.Listen("tcp", programAddress)
	if err != nil {
		return err
	}

	s := grpc.NewServer()
	for _, grpcService := range grpcServices {
		err = grpcService(s)
		if err != nil {
			return err
		}

	}

	go func() {
		if err := s.Serve(lis); err != nil {
			logger.WithField("error", err).Fatalf("Failed to serve")
		}
	}()

	return nil

}
