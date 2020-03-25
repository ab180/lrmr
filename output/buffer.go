package output

import (
	"github.com/pkg/errors"
	"github.com/therne/lrmr/lrdd"
	"sync"
)

// BufferedOutput wraps Output with buffering.
type BufferedOutput struct {
	buf    []*lrdd.Row
	offset int
	lock   sync.RWMutex
	output Output
}

func NewBufferedOutput(output Output, size int) *BufferedOutput {
	return &BufferedOutput{
		output: output,
		buf:    make([]*lrdd.Row, size),
	}
}

func (b *BufferedOutput) Write(d []*lrdd.Row) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	//log.Verbose("Start write (Offset: {}/{})", b.offset, len(b.buf))
	for len(d) > 0 {
		writeLen := min(len(d), len(b.buf)-b.offset)
		copy(b.buf[b.offset:b.offset+writeLen], d[:writeLen])

		b.offset += writeLen
		if b.offset == len(b.buf) {
			err := b.flush()
			if err != nil {
				return err
			}
		}
		d = d[writeLen:]
		//log.Verbose("  - Written {} rows (Offset: {}/{})", writeLen, b.offset, len(b.buf))
	}
	//log.Verbose("End write")
	return nil
}

func (b *BufferedOutput) flush() (err error) {
	if err = b.output.Write(b.buf[:b.offset]); err != nil {
		return
	}
	b.offset = 0
	return
}

func (b *BufferedOutput) Flush() error {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.flush()
}

func (b *BufferedOutput) Close() error {
	if err := b.Flush(); err != nil {
		return errors.Wrap(err, "flush")
	}
	return b.output.Close()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
