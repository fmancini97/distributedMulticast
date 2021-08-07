package sender

import (
	"context"
	"errors"
	pb "gitlab.com/f_mancini/distribuitedOrderedMulticast/sender/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
)

type Message struct {
	Metadata map[string]string
	Payload  []byte
}

type server struct {
	pb.UnimplementedSenderServer
}

var (
	streams map[string]chan Message
)

func init() {
	streams = make(map[string]chan Message)
}

func AddCh(id string) (ch chan Message, err error) {

	if _, ok := streams[id]; ok {
		err = errors.New("alreadyExists")
		return
	}

	ch = make(chan Message)
	streams[id] = ch

	return
}

func Register(s grpc.ServiceRegistrar) (err error) {
	pb.RegisterSenderServer(s, &server{})
	return
}

func (s *server) Send(_ context.Context, in *pb.Message) (*pb.Empty, error) {

	metadata := in.GetMetadata()

	id, exists := metadata["ID"]

	if !exists {
		log.Printf("Missing ID")
		return nil, status.Errorf(codes.InvalidArgument, "Missing ID in metadata field")
	}

	for k := range streams {
		log.Printf("key[%s]\n", k)
	}

	ch, exists := streams[id]

	if !exists {
		log.Printf("No stream for ID %s", id)
		return nil, status.Errorf(codes.NotFound, "Stream %s does not exists", id)
	}

	ch <- Message{in.Metadata, in.Payload}

	return &pb.Empty{}, nil
}
