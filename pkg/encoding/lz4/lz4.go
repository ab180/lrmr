package lz4

import (
	"io"
	"sync"

	"github.com/pierrec/lz4/v4"
	"google.golang.org/grpc/encoding"
)

const Name = "lz4"

func init() {
	c := &compressor{}
	encoding.RegisterCompressor(c)
}

type compressor struct {
}

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	zw := writerPool.Get().(*writer)
	zw.Writer.Reset(w)
	return zw, nil
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	zr := readerPool.Get().(*reader)
	zr.Reset(r)

	return zr, nil
}

func (c *compressor) Name() string {
	return Name
}

type writer struct {
	*lz4.Writer
}

func (w *writer) Close() error {
	defer writerPool.Put(w)
	return w.Writer.Close()
}

type reader struct {
	*lz4.Reader
}

func (z *reader) Read(p []byte) (n int, err error) {
	n, err = z.Reader.Read(p)
	if err == io.EOF {
		readerPool.Put(z)
	}
	return n, err
}

var (
	writerPool = sync.Pool{
		New: func() any {
			return &writer{
				Writer: lz4.NewWriter(nil),
			}
		},
	}
	readerPool = sync.Pool{
		New: func() any {
			return &reader{
				Reader: lz4.NewReader(nil),
			}
		},
	}
)
