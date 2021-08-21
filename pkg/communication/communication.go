package communication

type Message struct {
	Metadata map[string]string `json:"metadata"`
	Payload  []byte            `json:"payload"`
}

func NewMessage(payload []byte) Message {
	return Message{Metadata: make(map[string]string), Payload: payload}
}

type Sender interface {
	Send(streamId string, message Message) error
	Close() error
}

type Deliverer interface {
	Deliver(message Message) error
}
