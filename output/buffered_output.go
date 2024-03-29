package output

import (
	"github.com/ab180/lrmr/lrdd"
	"github.com/pkg/errors"
)

// BufferedOutput wraps Output with buffering.
type BufferedOutput struct {
	buf    []lrdd.Row
	offset int
	output Output
}

func NewBufferedOutput(output Output, size int) *BufferedOutput {
	if size == 0 {
		panic("buffer size cannot be 0.")
	}

	return &BufferedOutput{
		output: output,
		buf:    make([]lrdd.Row, size),
	}
}

func (b *BufferedOutput) Write(d []lrdd.Row) error {
	for len(d) > 0 {
		writeLen := min(len(d), len(b.buf)-b.offset)
		b.offset += copy((b.buf)[b.offset:], d[:writeLen])
		if b.offset == len(b.buf) {
			err := b.Flush()
			if err != nil {
				return err
			}
		}
		d = d[writeLen:]
	}
	return nil
}

func (b *BufferedOutput) Flush() error {
	if err := b.output.Write((b.buf)[:b.offset]); err != nil {
		return err
	}
	b.offset = 0
	return nil
}

func (b *BufferedOutput) Close() error {
	if err := b.Flush(); err != nil {
		return errors.Wrap(err, "flush")
	}
	b.buf = nil
	return b.output.Close()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
