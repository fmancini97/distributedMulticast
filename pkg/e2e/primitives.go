package e2e

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	e2egrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e/grpc"
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

func Register(s grpc.ServiceRegistrar) (err error) {
	e2egrpc.RegisterE2EServer(s, &server{})
	return
}
