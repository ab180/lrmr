package output

import "github.com/ab180/lrmr/lrdd"

var lrddRowsPool = newRowsPool()

func newRowsPool() *rowsPool {
	const poolSize = 1_000_000
	p := &rowsPool{
		pool: make(chan *[]*lrdd.Row, poolSize),
	}

	return p
}

type rowsPool struct {
	pool chan *[]*lrdd.Row
}

// Get returns a rows from the pool.
func (p *rowsPool) Get() *[]*lrdd.Row {
	var v *[]*lrdd.Row
	select {
	case v = <-p.pool:
	default:
		v = &[]*lrdd.Row{}
	}

	return v
}

// ResetAndPut resets the given rows and puts it back to the pool.
func (p *rowsPool) ResetAndPut(rows *[]*lrdd.Row) {
	select {
	case p.pool <- rows:
		*rows = (*rows)[:0]
	default:
	}
}
