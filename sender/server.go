package sender

import (
	"context"
	com "gitlab.com/f_mancini/distribuitedOrderedMulticast"
	pb "gitlab.com/f_mancini/distribuitedOrderedMulticast/sender/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
)

type server struct {
	pb.UnimplementedSenderServer
}

func (s *server) Send(_ context.Context, in *pb.Message) (*pb.Empty, error) {
	dlvs.mu.RLock()
	defer dlvs.mu.RUnlock()

	id := in.GetId()

	deliverer, exists := dlvs.streams[id]

	if !exists {
		log.Printf("No stream for ID %s", id)
		return nil, status.Errorf(codes.NotFound, "Stream %s does not exists", id)
	}

	err := deliverer.Deliver(com.Message{Metadata: in.Metadata, Payload: in.Payload})
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	return &pb.Empty{}, nil
}
