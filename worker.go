package goconsumer

import (
	"github.com/goinbox/golog"

	"sync"
)

type IWorker interface {
	Start(wg *sync.WaitGroup)
	Stop()
	Assign(line []byte) error
}

type LineProcessFunc func(line []byte) error

type BaseWorker struct {
	Logger golog.Logger
	Id     int

	lpf LineProcessFunc

	lineCh chan []byte
	stopCh chan bool
}

func NewBaseWorker() *BaseWorker {
	return &BaseWorker{
		Logger: new(golog.NoopLogger),

		stopCh: make(chan bool),
	}
}

func (b *BaseWorker) SetLogger(logger golog.Logger) {
	b.Logger = logger
}

func (b *BaseWorker) SetLineProcessFunc(lpf LineProcessFunc) {
	b.lpf = lpf
}

func (b *BaseWorker) SetLineCh(lineCh chan []byte) {
	b.lineCh = lineCh
}

func (b *BaseWorker) Start(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()

	if b.lineCh == nil {
		b.lineCh = make(chan []byte)
	}

	for {
		select {
		case line := <-b.lineCh:
			err := b.lpf(line)
			if err != nil {
				b.Logger.Error("processLineError", golog.ErrorField(err))
			}
		case <-b.stopCh:
			return
		}
	}
}

func (b *BaseWorker) Stop() {
	b.stopCh <- true
}

func (b *BaseWorker) Assign(line []byte) error {
	b.lineCh <- line

	return nil
}
