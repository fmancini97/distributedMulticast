package sender

import (
	"errors"
	com "gitlab.com/f_mancini/distribuitedOrderedMulticast"
	pb "gitlab.com/f_mancini/distribuitedOrderedMulticast/sender/service"
	"google.golang.org/grpc"
	"sync"
)

type deliverers struct {
	mu      sync.RWMutex
	streams map[string]com.Deliverer
}

var (
	dlvs deliverers
)

func init() {
	dlvs.streams = make(map[string]com.Deliverer)
}

func AddDeliverer(id string, deliverer com.Deliverer) (err error) {
	dlvs.mu.Lock()
	defer dlvs.mu.Unlock()

	if _, ok := dlvs.streams[id]; ok {
		err = errors.New("alreadyExists")
		return
	}

	dlvs.streams[id] = deliverer

	return
}

func Register(s grpc.ServiceRegistrar) (err error) {
	pb.RegisterSenderServer(s, &server{})
	return
}
