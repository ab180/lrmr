package input

import (
	"github.com/therne/lrmr/lrdd"
	"sync"
	"sync/atomic"
	"time"
)

type Reader struct {
	C chan []*lrdd.Row

	inputs    []Input
	lock      sync.RWMutex
	activeCnt int64
}

func NewReader(queueLen int) *Reader {
	return &Reader{
		C: make(chan []*lrdd.Row, queueLen),
	}
}

func (p *Reader) Add(in Input) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.inputs = append(p.inputs, in)
	atomic.AddInt64(&p.activeCnt, 1)
}

func (p *Reader) Done() {
	activeCnt := atomic.AddInt64(&p.activeCnt, -1)
	if activeCnt == 0 {
		close(p.C)
		p.inputs = nil
	}
}

func (p *Reader) Close() error {
	for c := atomic.LoadInt64(&p.activeCnt); c > 0; {
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}
