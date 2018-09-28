package goconsumer

import (
	"github.com/goinbox/golog"

	"sync"
)

type IWorker interface {
	Id() int
	Work(lineCh chan []byte, wg *sync.WaitGroup, stopCh chan bool)
}

type LineProcessFunc func(line []byte) error

type BaseWorker struct {
	Logger golog.ILogger

	id  int
	lpf LineProcessFunc
}

func NewBaseWorker() *BaseWorker {
	return &BaseWorker{
		Logger: new(golog.NoopLogger),
	}
}

func (b *BaseWorker) SetLogger(logger golog.ILogger) {
	b.Logger = logger
}

func (b *BaseWorker) SetId(id int) {
	b.id = id
}

func (b *BaseWorker) SetLineProcessFunc(lpf LineProcessFunc) {
	b.lpf = lpf
}

func (b *BaseWorker) Id() int {
	return b.id
}

func (b *BaseWorker) Work(lineCh chan []byte, wg *sync.WaitGroup, stopCh chan bool) {
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
