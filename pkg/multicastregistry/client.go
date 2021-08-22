package multicastregistry

import (
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicastregistry/registrygrpc"
	"google.golang.org/grpc"
)

func Connect(address string) (registrygrpc.RegistryClient, error) {
	logger := registryClientLog.WithField("address", address)

	logger.Debug("Connecting to registry server")

	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}

	return registrygrpc.NewRegistryClient(conn), nil
}
