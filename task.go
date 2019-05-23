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

	wg *sync.WaitGroup
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
	t.consumer.SetMessageCallback(t.consumerMessageCallback)

	return t
}

func (t *Task) SetDispatcher(dispatcher IDispatcher) *Task {
	t.dispatcher = dispatcher

	return t
}

func (t *Task) SetWorkerList(workerList []IWorker) *Task {
	t.workerList = workerList

	return t
}

func (t *Task) Start() {
	for _, worker := range t.workerList {
		go worker.Start(t.wg)

		t.wg.Add(1)
	}

	t.consumer.Start()
}

func (t *Task) Stop() {
	t.consumer.Stop()

	for _, worker := range t.workerList {
		worker.Stop()
	}

	t.wg.Wait()
}

func (t *Task) consumerMessageCallback(msg []byte) error {
	for _, line := range bytes.Split(msg, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) != 0 {
			i := t.dispatcher.Dispatch(line)
			_ = t.workerList[i].Assign(line)
		}
	}

	return nil
}
