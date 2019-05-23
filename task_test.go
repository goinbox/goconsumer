package goconsumer

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	DEMO_WORKER_NUM = 10
)

type DemoWorker struct {
	*BaseWorker
}

func (d *DemoWorker) LineProcessFunc(line []byte) error {
	idStr := strconv.Itoa(d.Id)
	fmt.Println("wid:" + idStr + " process line:" + string(line))

	return nil
}

type DemoConsumer struct {
	BaseConsumer

	stop bool
}

func (d *DemoConsumer) Start() {
	i := 0
	for {
		str := "This message is from DemoConsumer loop " + strconv.Itoa(i)
		_ = d.MessageCallback([]byte(str))

		i++
		time.Sleep(time.Second * 1)
		if d.stop {
			return
		}
	}
}

func (d *DemoConsumer) Stop() {
	d.stop = true
}

func TestSimpleConsumerTask(t *testing.T) {
	lineCh := make(chan []byte)
	consumer := new(DemoConsumer)
	dispatcher := NewSimpleDispatcher(DEMO_WORKER_NUM)
	workerList := make([]IWorker, DEMO_WORKER_NUM)
	for i := 0; i < DEMO_WORKER_NUM; i++ {
		worker := &DemoWorker{NewBaseWorker()}
		worker.Id = i
		worker.SetLineProcessFunc(worker.LineProcessFunc)
		worker.SetLineCh(lineCh)
		workerList[i] = worker
	}

	task := NewTask("Demo").
		SetConsumer(consumer).
		SetDispatcher(dispatcher).
		SetWorkerList(workerList)

	go task.Start()
	time.Sleep(time.Second * 10)
	task.Stop()
}

func specifyDispatchLineFunc(line []byte) int {
	item := strings.Split(string(line), " ")
	vs := item[len(item)-1]
	v, _ := strconv.Atoi(vs)

	return v % DEMO_WORKER_NUM
}

func TestSpecifyConsumerTask(t *testing.T) {
	consumer := new(DemoConsumer)
	workerList := make([]IWorker, 10)
	for i := 0; i < 10; i++ {
		worker := &DemoWorker{NewBaseWorker()}
		worker.Id = i
		worker.SetLineProcessFunc(worker.LineProcessFunc)
		workerList[i] = worker
	}
	dispatcher := NewSpecifyDispatcher(specifyDispatchLineFunc)

	task := NewTask("Demo").
		SetConsumer(consumer).
		SetDispatcher(dispatcher).
		SetWorkerList(workerList)

	go task.Start()
	time.Sleep(time.Second * 10)
	task.Stop()
}
