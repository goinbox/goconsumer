package goconsumer

import (
	"github.com/goinbox/golog"

	"bytes"
	"sync"
	"time"
)

type Task struct {
	Name string

	logger   golog.ILogger
	consumer IConsumer

	workerNum int
	nwf       NewWorkerFunc
	wg        *sync.WaitGroup

	lineCh chan []byte
	stopCh chan bool
}

func NewTask(name string) *Task {
	return &Task{
		Name: name,

		logger: new(golog.NoopLogger),
	}
}

func (t *Task) SetLogger(logger golog.ILogger) *Task {
	t.logger = logger

	return t
}

func (t *Task) SetConsumer(consumer IConsumer) *Task {
	t.consumer = consumer
	t.consumer.SetHandleFunc(t.consumerHandleFunc)

	return t
}

func (t *Task) SetWorker(workerNum int, nwf NewWorkerFunc) *Task {
	t.workerNum = workerNum
	t.lineCh = make(chan []byte, workerNum)
	t.nwf = nwf

	t.wg = new(sync.WaitGroup)
	t.stopCh = make(chan bool, workerNum)

	return t
}

func (t *Task) Start() {
	t.startWorker()
	t.consumer.Start()
}

func (t *Task) Stop() {
	t.stopWorker()
	t.consumer.Stop()
}

func (t *Task) consumerHandleFunc(message IMessage) error {
	for _, line := range bytes.Split(message.Body(), []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) != 0 {
			t.lineCh <- line
		}
	}

	return nil
}

func (t *Task) startWorker() {
	for i := 0; i < t.workerNum; i++ {
		worker := t.nwf()
		worker.SetWorkId(i + 1)
		worker.SetLogger(t.logger)

		go func() {
			worker.Work(t.wg, t.lineCh, t.stopCh)
		}()

		t.wg.Add(1)
	}
}

func (t *Task) stopWorker() {
	t.consumer.Stop()

	for len(t.lineCh) != 0 {
		time.Sleep(time.Second * 1)
	}

	for i := 0; i < t.workerNum; i++ {
		t.stopCh <- true
	}

	t.wg.Wait()
}
