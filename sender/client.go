package sender

import (
	"context"
	"errors"
	dom "gitlab.com/f_mancini/distribuitedOrderedMulticast"
	pb "gitlab.com/f_mancini/distribuitedOrderedMulticast/sender/service"
	sender "gitlab.com/f_mancini/distribuitedOrderedMulticast/sender/service"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

type connections struct {
	connections map[string]*senderMetadata
	mu          sync.Mutex
}

var (
	conns connections
)

func init() {
	log.Println("Initializing package")
	conns.connections = make(map[string]*senderMetadata)
}

type senderMetadata struct {
	address string
	conn    *grpc.ClientConn
	client  sender.SenderClient
	counter int
	delay   int // Seconds
	rnd     *rand.Rand
}

type GrpcSender struct {
	meta *senderMetadata
}

func Connect(address string) (*GrpcSender, error) {
	return ConnectWithDelay(address, 0)
}

func ConnectWithDelay(address string, delay int) (*GrpcSender, error) {
	conns.mu.Lock()
	defer conns.mu.Unlock()

	// Check if connection already exists
	gs, ok := conns.connections[address]
	if !ok {
		conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return nil, err
		}
		gs = new(senderMetadata)
		gs.client = pb.NewSenderClient(conn)
		gs.conn = conn
		gs.counter = 0
		gs.address = address
		gs.delay = delay
		if delay > 0 {
			gs.rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
		}

		conns.connections[address] = gs

	}
	gs.counter++

	return &GrpcSender{gs}, nil
}

func (gs *GrpcSender) Send(streamId string, message dom.Message) error {

	if gs.meta == nil {
		return errors.New("connectionClosed")
	}

	if gs.meta.delay > 0 {
		time.Sleep(time.Duration(gs.meta.rnd.Intn(gs.meta.delay)) * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := gs.meta.client.Send(ctx, &pb.Message{Id: streamId, Metadata: message.Metadata, Payload: message.Payload})
	return err
}

func (gs *GrpcSender) Close() error {

	meta := gs.meta

	if meta == nil {
		return errors.New("connectionClosed")
	}

	conns.mu.Lock()
	defer conns.mu.Unlock()

	gs.meta = nil

	meta.counter--
	if meta.counter == 0 {
		delete(conns.connections, meta.address)
		err := meta.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
