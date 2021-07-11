package input

import (
	"sync"

	"github.com/ab180/lrmr/lrdd"
	"go.uber.org/atomic"
)

type Reader struct {
	C chan []*lrdd.Row

	activeCnt atomic.Int64
	once      sync.Once
}

func NewReader(queueLen int) *Reader {
	return &Reader{
		C: make(chan []*lrdd.Row, queueLen),
	}
}

func (p *Reader) Add() {
	p.activeCnt.Inc()
}

func (p *Reader) Done() {
	newActiveCnt := p.activeCnt.Dec()
	if newActiveCnt == 0 {
		p.Close()
	}
}

func (p *Reader) Close() {
	p.once.Do(func() {
		close(p.C)
	})
}
