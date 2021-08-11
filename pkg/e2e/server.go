package e2e

import (
	"context"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	e2egrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
)

type server struct {
	e2egrpc.UnimplementedE2EServer
}

func (s *server) Send(_ context.Context, in *e2egrpc.Message) (*e2egrpc.Empty, error) {
	dlvs.mu.RLock()
	defer dlvs.mu.RUnlock()

	id := in.GetId()

	deliverer, exists := dlvs.streams[id]

	if !exists {
		log.Printf("No stream for ID %s", id)
		return nil, status.Errorf(codes.NotFound, "Stream %s does not exists", id)
	}

	err := deliverer.Deliver(communication.Message{Metadata: in.Metadata, Payload: in.Payload})
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	return &e2egrpc.Empty{}, nil
}
