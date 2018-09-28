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
	idStr := strconv.Itoa(d.Id())
	fmt.Println("wid:" + idStr + " process line:" + string(line))

	return nil
}

type DemoMessage struct {
	body []byte
}

func (d *DemoMessage) Body() []byte {
	return d.body
}

type DemoConsumer struct {
	hf ConsumerHandleFunc
}

func (d *DemoConsumer) SetHandleFunc(hf ConsumerHandleFunc) {
	d.hf = hf
}

func (d *DemoConsumer) Start() {
	for i := 0; i < 100; i++ {
		str := "This message is from DemoConsumer loop " + strconv.Itoa(i)
		d.hf(&DemoMessage{[]byte(str)})
	}

	time.Sleep(time.Second * 1)
}

func (d *DemoConsumer) Stop() {

}

func TestSimpleConsumerTask(t *testing.T) {
	consumer := new(DemoConsumer)
	dispatcher := NewSimpleDispatcher(DEMO_WORKER_NUM)
	workerList := make([]IWorker, DEMO_WORKER_NUM)
	for i := 0; i < DEMO_WORKER_NUM; i++ {
		worker := &DemoWorker{NewBaseWorker()}
		worker.SetId(i)
		worker.SetLineProcessFunc(worker.LineProcessFunc)
		workerList[i] = worker
	}

	task := NewTask("Demo")
	task.SetConsumer(consumer).
		SetDispatcher(dispatcher).
		SetWorkerList(workerList).
		Start()
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
		worker.SetId(i)
		worker.SetLineProcessFunc(worker.LineProcessFunc)
		workerList[i] = worker
	}
	dispatcher := NewSpecifyDispatcher(workerList, 10, specifyDispatchLineFunc)

	task := NewTask("Demo")
	task.SetConsumer(consumer).
		SetDispatcher(dispatcher).
		SetWorkerList(workerList).
		Start()
}
