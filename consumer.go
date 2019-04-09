package goconsumer

type IMessage interface {
	Body() []byte
}

type ConsumerHandleFunc func(message IMessage) error

type IConsumer interface {
	SetHandleFunc(hf ConsumerHandleFunc)
	Start()
	Stop()
}

type BaseConsumer struct {
	HandleFunc ConsumerHandleFunc
}

func (b *BaseConsumer) SetHandleFunc(hf ConsumerHandleFunc) {
	b.HandleFunc = hf
}
