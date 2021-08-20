package multicasttypemapper

import (
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicast"
	registrygrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicastregistry/grpc"
)

var Type = map[registrygrpc.MulticastType]map[bool]multicast.Type{
	registrygrpc.MulticastType_BMULTICAST: {
		true:  multicast.Type_BMULTICAST,
		false: multicast.Type_BMULTICAST,
	},
	registrygrpc.MulticastType_FIFOMULTICAST: {
		true:  multicast.Type_FIFOMULTICAST,
		false: multicast.Type_FIFOMULTICAST,
	},
	registrygrpc.MulticastType_TOMULTICASTSEQ: {
		true:  multicast.Type_TOMULTICASTSEQMASTER,
		false: multicast.Type_TOMULTICASTSEQWORKER,
	},
	registrygrpc.MulticastType_TOFIFOMULTICASTSEQ: {
		true:  multicast.Type_TOFIFOMULTICASTSEQMASTER,
		false: multicast.Type_TOFIFOMULTICASTSEQWORKER,
	},
	registrygrpc.MulticastType_TOMULTICASTSC: {
		true:  multicast.Type_TOMULTICASTSC,
		false: multicast.Type_TOMULTICASTSC,
	},
	registrygrpc.MulticastType_COMULTICAST: {
		true:  multicast.Type_COMULTICAST,
		false: multicast.Type_COMULTICAST,
	},
	registrygrpc.MulticastType_TOCOMULTICAST: {
		true:  multicast.Type_TOCOMULTICASTMASTER,
		false: multicast.Type_TOCOMULTICASTWORKER,
	},
}
