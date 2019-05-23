package goconsumer

type ConsumerMessageCallback func(msg []byte) error

type IConsumer interface {
	SetMessageCallback(hf ConsumerMessageCallback)
	Start()
	Stop()
}

type BaseConsumer struct {
	MessageCallback ConsumerMessageCallback
}

func (b *BaseConsumer) SetMessageCallback(mcb ConsumerMessageCallback) {
	b.MessageCallback = mcb
}
