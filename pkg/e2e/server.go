package e2e

import (
	"context"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e/e2egrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type server struct {
	e2egrpc.UnimplementedE2EServer
}

func (s *server) Send(ctx context.Context, in *e2egrpc.Message) (*e2egrpc.Empty, error) {

	source, _ := peer.FromContext(ctx)

	id := in.GetId()

	logger := e2eLogServer.WithFields(log.Fields{"user_ip": source.Addr, "stream_id": id})
	logger.WithFields(log.Fields{"metadata": in.Metadata, "payload": in.Metadata}).Debug("New message")

	logger.Info("Processing new message")

	dlvs.mu.RLock()
	defer dlvs.mu.RUnlock()

	deliverer, exists := dlvs.streams[id]

	if !exists {
		logger.Error("No stream available")
		return nil, status.Errorf(codes.NotFound, "Stream %s does not exists", id)
	}

	err := deliverer.Deliver(communication.Message{Metadata: in.Metadata, Payload: in.Payload})
	if err != nil {
		log.Error("Error while delivering message")
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	logger.Info("Message processed")

	return &e2egrpc.Empty{}, nil
}
