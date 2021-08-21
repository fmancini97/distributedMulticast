package e2e

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	e2egrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e/e2egrpc"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/multicast"
	"google.golang.org/grpc"
	"sync"
)

type deliverers struct {
	mu      sync.RWMutex
	streams map[string]communication.Deliverer
}

var (
	dlvs   deliverers
	e2eLog *log.Entry
)

func init() {
	dlvs.streams = make(map[string]communication.Deliverer)
	e2eLog = log.WithField("layer", "e2e")
}

func AddDeliverer(id string, deliverer communication.Deliverer) (err error) {
	dlvs.mu.Lock()
	defer dlvs.mu.Unlock()

	if _, ok := dlvs.streams[id]; ok {
		err = errors.New("alreadyExists")
		return
	}

	dlvs.streams[id] = deliverer

	return
}

func RemoveDeliverer(id string) (err error) {
	dlvs.mu.Lock()
	defer dlvs.mu.Unlock()

	if _, ok := dlvs.streams[id]; !ok {
		err = errors.New("notExists")
		return
	}

	delete(dlvs.streams, id)

	return nil
}

func Register(s grpc.ServiceRegistrar) (err error) {
	e2egrpc.RegisterE2EServer(s, &server{})
	return
}

func InitializeConnection(id, address string, delay uint) (*multicast.E2EMember, error) {
	grpcSender, err := ConnectWithDelay(address, delay)
	if err != nil {
		return nil, err
	}

	return &multicast.E2EMember{
		Id:      id,
		Address: address,
		Sender:  grpcSender,
	}, nil
}

func InitializeConnections(addresses []string, delay uint) ([]*multicast.E2EMember, error) {
	// Taking other connections

	e2eConnections := make([]*multicast.E2EMember, 0)

	for _, address := range addresses {
		e2eConnection, err := InitializeConnection(address, address, delay)
		if err != nil {
			return nil, err
		}
		e2eConnections = append(e2eConnections, e2eConnection)
	}

	return e2eConnections, nil
}
