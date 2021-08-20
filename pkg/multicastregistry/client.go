package multicastregistry

import (
	registrygrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicastregistry/grpc"
	"google.golang.org/grpc"
)

func Connect(address string) (registrygrpc.RegistryClient, error) {
	logger := registryLog.WithField("address", address)

	logger.Debug("Connecting")

	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}

	return registrygrpc.NewRegistryClient(conn), nil
}
