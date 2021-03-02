package input

import (
	"github.com/ab180/lrmr/lrdd"
	"go.uber.org/atomic"

	"sync"
)

type Reader struct {
	C chan []*lrdd.Row

	inputs    []Input
	lock      sync.RWMutex
	activeCnt atomic.Int64
	closed    atomic.Bool
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
	p.activeCnt.Inc()
}

func (p *Reader) Done() {
	newActiveCnt := p.activeCnt.Dec()
	if newActiveCnt == 0 {
		p.Close()
	}
}

func (p *Reader) Close() {
	if swapped := p.closed.CAS(false, true); !swapped {
		// p.closed was true
		return
	}
	// with CAS, only a goroutine can enter here
	close(p.C)
	p.inputs = nil
}
