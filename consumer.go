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
