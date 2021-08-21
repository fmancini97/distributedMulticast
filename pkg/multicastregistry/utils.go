package multicastregistry

import (
	registrygrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicastregistry/registryrpc"
	"google.golang.org/grpc"
)

var MulticastType = map[string]registrygrpc.MulticastType{
	"BMULTICAST":     registrygrpc.MulticastType_BMULTICAST,
	"FIFOMULTICAST":  registrygrpc.MulticastType_FIFOMULTICAST,
	"TOMULTICASTSEQ": registrygrpc.MulticastType_TOMULTICASTSEQ,
	"TOMULTICASTSC":  registrygrpc.MulticastType_TOMULTICASTSC,
	"COMULTICAST":    registrygrpc.MulticastType_COMULTICAST,
	"TOCOMULTICAST":  registrygrpc.MulticastType_TOCOMULTICAST,
}

var MulticastTypes []string

func init() {
	MulticastTypes = make([]string, 0)

	for multicastType, _ := range MulticastType {
		MulticastTypes = append(MulticastTypes, multicastType)
	}
}

func Register(s grpc.ServiceRegistrar) (err error) {
	registrygrpc.RegisterRegistryServer(s, &server{})
	return
}
