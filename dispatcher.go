package goconsumer

type IDispatcher interface {
	Dispatch(line []byte) int
}

type simpleDispatcher struct {
	index int
	max   int
}

func NewSimpleDispatcher(workerNum int) *simpleDispatcher {
	return &simpleDispatcher{
		index: 0,
		max:   workerNum,
	}
}

func (s *simpleDispatcher) Dispatch(line []byte) int {
	defer func() {
		s.index = (s.index + 1) % s.max
	}()

	return s.index
}

type DispatchFunc func(line []byte) int

type specifyDispatcher struct {
	dlf DispatchFunc
}

func NewSpecifyDispatcher(dlf DispatchFunc) *specifyDispatcher {
	return &specifyDispatcher{
		dlf: dlf,
	}
}

func (s *specifyDispatcher) Dispatch(line []byte) int {
	return s.dlf(line)
}
