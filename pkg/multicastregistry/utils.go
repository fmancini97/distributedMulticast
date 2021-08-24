// Package multicastregistry implements a server for discovering multicast groups and registering nodes to groups
package multicastregistry

import (
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distributedMulticast/pkg/multicastregistry/registrygrpc"
	"google.golang.org/grpc"
)

var MulticastType = map[string]registrygrpc.MulticastType{
	"BMULTICAST":         registrygrpc.MulticastType_BMULTICAST,
	"FIFOMULTICAST":      registrygrpc.MulticastType_FIFOMULTICAST,
	"TOMULTICASTSEQ":     registrygrpc.MulticastType_TOMULTICASTSEQ,
	"TOFIFOMULTICASTSEQ": registrygrpc.MulticastType_TOFIFOMULTICASTSEQ,
	"TOMULTICASTSC":      registrygrpc.MulticastType_TOMULTICASTSC,
	"COMULTICAST":        registrygrpc.MulticastType_COMULTICAST,
	"TOCOMULTICAST":      registrygrpc.MulticastType_TOCOMULTICAST,
}

var (
	MulticastTypes    []string
	registryServerLog = log.WithField("app", "registryServer")
	registryClientLog = log.WithField("app", "registryClient")
)

func init() {
	MulticastTypes = make([]string, 0)

	for multicastType := range MulticastType {
		MulticastTypes = append(MulticastTypes, multicastType)
	}
}

func Register(s grpc.ServiceRegistrar) (err error) {
	registrygrpc.RegisterRegistryServer(s, &server{})
	return
}
