package e2e

import (
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	e2egrpc "gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e/grpc"
	"google.golang.org/grpc"
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
	conns.connections = make(map[string]*senderMetadata)
}

type senderMetadata struct {
	address string
	conn    *grpc.ClientConn
	client  e2egrpc.E2EClient
	counter int
	delay   int // Millisecond
	rnd     *rand.Rand
}

type GrpcSender struct {
	meta *senderMetadata
}

func Connect(address string) (*GrpcSender, error) {
	return ConnectWithDelay(address, 0)
}

func ConnectWithDelay(address string, delay int) (*GrpcSender, error) {
	logger := e2eLog.WithField("address", address)

	logger.Debug("Connecting")

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
		gs.client = e2egrpc.NewE2EClient(conn)
		gs.conn = conn
		gs.counter = 0
		gs.address = address
		gs.delay = delay
		if delay > 0 {
			gs.rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
		}

		conns.connections[address] = gs
	} else {
		log.WithField("connection_info", gs).Debug("Connection already exists")
	}
	gs.counter++

	return &GrpcSender{gs}, nil
}

func (gs *GrpcSender) Send(streamId string, message communication.Message) error {
	logger := e2eLog.WithField("stream_id", streamId)
	logger.WithField("message", message).Debug("Sending message")
	if gs.meta == nil {
		return errors.New("connectionClosed")
	}

	if gs.meta.delay > 0 {
		time.Sleep(time.Duration(gs.meta.rnd.Intn(gs.meta.delay)) * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000)
	defer cancel()

	_, err := gs.meta.client.Send(ctx, &e2egrpc.Message{Id: streamId, Metadata: message.Metadata, Payload: message.Payload})
	return err
}

func (gs *GrpcSender) Close() error {

	meta := gs.meta

	if meta == nil {
		return errors.New("connectionClosed")
	}

	logger := e2eLog.WithField("connection", meta)

	logger.Debug("Closing")
	conns.mu.Lock()
	defer conns.mu.Unlock()

	gs.meta = nil

	meta.counter--
	if meta.counter == 0 {
		logger.Debug()
		delete(conns.connections, meta.address)
		err := meta.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
