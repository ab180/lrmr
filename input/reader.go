package input

import (
	"github.com/ab180/lrmr/lrdd"
	"github.com/airbloc/logger"
	"go.uber.org/atomic"
)

type Reader struct {
	C chan []*lrdd.Row

	expectedTotalInputs int64

	activeCnt atomic.Int64
	totalCnt  atomic.Int64
	closed    atomic.Bool
}

func NewReader(queueLen, expectedTotalInputs int) *Reader {
	return &Reader{
		C:                   make(chan []*lrdd.Row, queueLen),
		expectedTotalInputs: int64(expectedTotalInputs),
	}
}

func (p *Reader) Add() {
	p.activeCnt.Inc()
	p.totalCnt.Inc()
}

func (p *Reader) Write(chunk []*lrdd.Row) {
	if p.closed.Load() {
		return
	}
	p.C <- chunk
}

func (p *Reader) Done() {
	newActiveCnt := p.activeCnt.Dec()
	if newActiveCnt == 0 && p.totalCnt.Load() == p.expectedTotalInputs {
		p.Close()
	}
	logger.New("input").Verbose(" -> Total: {}, Active: {}", p.totalCnt.Load(), newActiveCnt)
}

func (p *Reader) Close() {
	// this ensures input to be closed only once
	if swapped := p.closed.CAS(false, true); !swapped {
		return
	}
	close(p.C)
}
