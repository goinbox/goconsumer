package goconsumer

import (
	"github.com/goinbox/golog"

	"bytes"
	"sync"
)

type Task struct {
	Name string

	logger     golog.ILogger
	consumer   IConsumer
	dispatcher IDispatcher
	workerList []IWorker

	wg     *sync.WaitGroup
	stopCh chan bool
}

func NewTask(name string) *Task {
	return &Task{
		Name: name,

		logger: new(golog.NoopLogger),

		wg: new(sync.WaitGroup),
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

func (t *Task) SetDispatcher(dispatcher IDispatcher) *Task {
	t.dispatcher = dispatcher

	return t
}

func (t *Task) SetWorkerList(workerList []IWorker) *Task {
	t.workerList = workerList

	t.stopCh = make(chan bool, len(workerList))

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
			t.dispatcher.DispatchLine(line)
		}
	}

	return nil
}

func (t *Task) startWorker() {
	for _, worker := range t.workerList {
		lineCh := t.dispatcher.DispatchLineChannel(worker)
		go worker.Work(lineCh, t.wg, t.stopCh)

		t.wg.Add(1)
	}
}

func (t *Task) stopWorker() {
	t.consumer.Stop()
	t.dispatcher.Wait()

	for i := 0; i < len(t.workerList); i++ {
		t.stopCh <- true
	}

	t.wg.Wait()
}
