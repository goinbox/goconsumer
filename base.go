package consumer

import (
	"github.com/goinbox/golog"

	"sync"
)

type IMessage interface {
	Body() []byte
}

type ConsumerHandleFunc func(message IMessage) error

type IConsumer interface {
	SetHandleFunc(hf ConsumerHandleFunc)
	Start()
	Stop()
}

type IWorker interface {
	Work(id int, wg *sync.WaitGroup, lineCh chan []byte, stopCh chan bool)
}

type LineProcessFunc func(line []byte) error

type BaseWorker struct {
	Logger golog.ILogger
	Id     int

	lpf LineProcessFunc
}

func NewBaseWorker() *BaseWorker {
	return &BaseWorker{
		Logger: new(golog.NoopLogger),
	}
}

func (b *BaseWorker) SetLogger(logger golog.ILogger) *BaseWorker {
	b.Logger = logger

	return b
}

func (b *BaseWorker) SetLineProcessFunc(lpf LineProcessFunc) *BaseWorker {
	b.lpf = lpf

	return b
}

func (b *BaseWorker) Work(id int, wg *sync.WaitGroup, lineCh chan []byte, stopCh chan bool) {
	defer func() {
		wg.Done()
	}()

	b.Id = id

	for {
		select {
		case line := <-lineCh:
			err := b.lpf(line)
			if err != nil {
				b.Logger.Error([]byte("processLineError:" + err.Error()))
			}
		case <-stopCh:
			return
		}
	}
}
