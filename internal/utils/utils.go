package utils

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicast"
	"google.golang.org/grpc"
	"net"
)

func InitializeServer(programAddress string) error {
	logger := log.WithField("address", programAddress)
	logger.Debug("Initializing server")

	lis, err := net.Listen("tcp", programAddress)
	if err != nil {
		return err
	}

	s := grpc.NewServer()
	err = e2e.Register(s)
	if err != nil {
		return err
	}

	go func() {
		if err := s.Serve(lis); err != nil {
			logger.WithField("error", err).Fatalf("Failed to serve")
		}
	}()

	return nil

}

func InitializeConnections(addresses []string, delay int) ([]multicast.E2EMember, error) {
	// Taking other connections

	connections := make([]multicast.E2EMember, 0)

	for _, address := range addresses {
		grpcSender, err := e2e.ConnectWithDelay(address, delay)
		if err != nil {
			return nil, err
		}

		connections = append(connections, multicast.E2EMember{Address: address, Sender: grpcSender, Id: address})
	}

	return connections, nil
}
