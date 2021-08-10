package distribuitedOrderedMulticast

type Message struct {
	Metadata map[string]string
	Payload  []byte
}

type Sender interface {
	Send(streamId string, message Message) error
	Close() error
}

type Deliverer interface {
	Deliver(message Message) error
}
