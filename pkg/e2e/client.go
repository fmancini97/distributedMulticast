package e2e

import (
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/communication"
	"gitlab.com/f_mancini/distribuitedOrderedMulticast/pkg/e2e/e2egrpc"
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
	delay   uint // Millisecond
	rnd     *rand.Rand
}

type GrpcSender struct {
	meta *senderMetadata
}

func Connect(address string) (*GrpcSender, error) {
	return ConnectWithDelay(address, 0)
}

func ConnectWithDelay(address string, delay uint) (*GrpcSender, error) {
	logger := e2eLogClient.WithField("server_address", address)

	logger.WithField("delay", delay).Debug("Connecting")

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
		logger.WithField("connection_info", gs).Debug("Connection already exists")
	}
	gs.counter++

	logger.Debug("Connected")

	return &GrpcSender{gs}, nil
}

func (gs *GrpcSender) Send(streamId string, message communication.Message) error {
	logger := e2eLogClient.WithField("streamId", streamId)
	logger.WithFields(log.Fields{"metadata": message.Metadata, "payload": message.Payload}).Debug("Sending message")
	if gs.meta == nil {
		return errors.New("closed connection")
	}

	if gs.meta.delay > 0 {
		time.Sleep(time.Duration(gs.meta.rnd.Intn(int(gs.meta.delay))) * time.Millisecond)
	}

	_, err := gs.meta.client.Send(context.Background(), &e2egrpc.Message{Id: streamId, Metadata: message.Metadata, Payload: message.Payload})

	if err != nil {
		logger.Debug("Message sent")
	}

	return err
}

func (gs *GrpcSender) Close() error {

	meta := gs.meta

	if meta == nil {
		return errors.New("connectionClosed")
	}

	logger := e2eLogClient.WithField("server_address", meta.address)

	logger.WithField("connection", meta).Debug("Closing connection")

	conns.mu.Lock()
	defer conns.mu.Unlock()

	gs.meta = nil

	meta.counter--
	if meta.counter == 0 {
		logger.Debug("No more clients which use connection... deallocating")
		delete(conns.connections, meta.address)
		err := meta.conn.Close()
		if err != nil {
			return err
		}
	}

	logger.Debug("Connection closed successfully")

	return nil
}
