package goconsumer

import (
	"time"
)

type IDispatcher interface {
	DispatchLine(line []byte)
	DispatchLineChannel(worker IWorker) chan []byte
	Wait()
}

type simpleDispatcher struct {
	lineCh chan []byte
}

func NewSimpleDispatcher(workerNum int) *simpleDispatcher {
	return &simpleDispatcher{
		lineCh: make(chan []byte, workerNum),
	}
}

func (s *simpleDispatcher) DispatchLine(line []byte) {
	s.lineCh <- line
}

func (s *simpleDispatcher) DispatchLineChannel(worker IWorker) chan []byte {
	return s.lineCh
}

func (s *simpleDispatcher) Wait() {
	for len(s.lineCh) != 0 {
		time.Sleep(time.Second * 1)
	}
}

type DispatchLineFunc func(line []byte) int

type specifyDispatcher struct {
	lineChMap map[int]chan []byte
	dlf       DispatchLineFunc
}

func NewSpecifyDispatcher(workerList []IWorker, bufLen int, dlf DispatchLineFunc) *specifyDispatcher {
	s := &specifyDispatcher{
		lineChMap: make(map[int]chan []byte),
		dlf:       dlf,
	}

	for _, worker := range workerList {
		s.lineChMap[worker.Id()] = make(chan []byte, bufLen)
	}

	return s
}

func (s *specifyDispatcher) DispatchLine(line []byte) {
	i := s.dlf(line)
	lineCh, ok := s.lineChMap[i]
	if ok {
		lineCh <- line
	}
}

func (s *specifyDispatcher) DispatchLineChannel(worker IWorker) chan []byte {
	return s.lineChMap[worker.Id()]
}

func (s *specifyDispatcher) Wait() {
	for _, lineCh := range s.lineChMap {
		for len(lineCh) != 0 {
			time.Sleep(time.Second * 1)
		}
	}
}
