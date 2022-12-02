package input

import (
	"github.com/ab180/lrmr/lrdd"
	"go.uber.org/atomic"
)

type Reader struct {
	C     chan []*lrdd.Row
	rowID lrdd.RowID

	activeCnt atomic.Int64
	closed    atomic.Bool
}

func NewReader(queueLen int, rowID lrdd.RowID) *Reader {
	return &Reader{
		C:     make(chan []*lrdd.Row, queueLen),
		rowID: rowID,
	}
}

func (p *Reader) Add() {
	p.activeCnt.Inc()
}

func (p *Reader) Write(chunk []*lrdd.Row) {
	if p.closed.Load() {
		return
	}
	p.C <- chunk
}

func (p *Reader) Done() {
	newActiveCnt := p.activeCnt.Dec()
	if newActiveCnt == 0 {
		p.Close()
	}
}

func (p *Reader) Close() {
	// this ensures input to be closed only once
	if swapped := p.closed.CAS(false, true); !swapped {
		return
	}
	close(p.C)
}

func (p *Reader) RowID() lrdd.RowID {
	return p.rowID
}
