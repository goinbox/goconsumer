package goconsumer

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

type DemoWorker struct {
	*BaseWorker
}

func NewDemoWorker() IWorker {
	worker := &DemoWorker{NewBaseWorker()}
	worker.SetLineProcessFunc(worker.LineProcessFunc)

	return worker
}

func (d *DemoWorker) LineProcessFunc(line []byte) error {
	idStr := strconv.Itoa(d.Id)
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

func TestConsumerTask(t *testing.T) {
	task := NewTask("Demo")
	consumer := new(DemoConsumer)

	task.SetConsumer(consumer).
		SetWorker(10, NewDemoWorker).
		Start()
}
