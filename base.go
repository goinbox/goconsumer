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

type NewWorkerFunc func() IWorker

type IWorker interface {
	SetWorkId(id int)
	SetLogger(logger golog.ILogger)

	Work(wg *sync.WaitGroup, lineCh chan []byte, stopCh chan bool)
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

func (b *BaseWorker) SetWorkId(id int) {
	b.Id = id
}

func (b *BaseWorker) SetLogger(logger golog.ILogger) {
	b.Logger = logger
}

func (b *BaseWorker) SetLineProcessFunc(lpf LineProcessFunc) {
	b.lpf = lpf
}

func (b *BaseWorker) Work(wg *sync.WaitGroup, lineCh chan []byte, stopCh chan bool) {
	defer func() {
		wg.Done()
	}()

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
