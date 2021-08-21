package multicasttypemapper

import (
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicast"
	registrygrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicastregistry/registryrpc"
)

var Type = map[registrygrpc.MulticastType]map[bool]multicast.Type{
	registrygrpc.MulticastType_BMULTICAST: {
		true:  multicast.TypeBMULTICAST,
		false: multicast.TypeBMULTICAST,
	},
	registrygrpc.MulticastType_FIFOMULTICAST: {
		true:  multicast.TypeFIFOMULTICAST,
		false: multicast.TypeFIFOMULTICAST,
	},
	registrygrpc.MulticastType_TOMULTICASTSEQ: {
		true:  multicast.TypeTOMULTICASTSEQMASTER,
		false: multicast.TypeTOMULTICASTSEQWORKER,
	},
	registrygrpc.MulticastType_TOFIFOMULTICASTSEQ: {
		true:  multicast.TypeTOFIFOMULTICASTSEQMASTER,
		false: multicast.TypeTOFIFOMULTICASTSEQWORKER,
	},
	registrygrpc.MulticastType_TOMULTICASTSC: {
		true:  multicast.TypeTOMULTICASTSC,
		false: multicast.TypeTOMULTICASTSC,
	},
	registrygrpc.MulticastType_COMULTICAST: {
		true:  multicast.TypeCOMULTICAST,
		false: multicast.TypeCOMULTICAST,
	},
	registrygrpc.MulticastType_TOCOMULTICAST: {
		true:  multicast.TypeTOCOMULTICASTMASTER,
		false: multicast.TypeTOCOMULTICASTWORKER,
	},
}
