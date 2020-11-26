package test

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
	"github.com/airbloc/logger"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

type jsonDecoder struct{}

func DecodeJSON() lrmr.FlatMapper {
	return &jsonDecoder{}
}

func (l *jsonDecoder) FlatMap(ctx lrmr.Context, in *lrdd.Row) (result []*lrdd.Row, err error) {
	var path string
	in.UnmarshalValue(&path)

	logger.New("jsondecoder").Verbose("Opening {}", filepath.Base(path))

	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open file")
	}
	ctx.AddMetric("Files", 1)

	r := bufio.NewReader(file)
	for {
		line, err := readline(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		msg := map[string]interface{}{}
		if err := jsoniter.Unmarshal(line, &msg); err != nil {
			return nil, err
		}
		appID := strconv.Itoa(int(msg["appID"].(float64)))
		result = append(result, lrdd.KeyValue(appID, msg))
	}
	return result, file.Close()
}

func readline(r *bufio.Reader) (line []byte, err error) {
	var isPrefix = true
	var ln []byte
	var buf bytes.Buffer
	for isPrefix && err == nil {
		ln, isPrefix, err = r.ReadLine()
		buf.Write(ln)
	}
	line = buf.Bytes()
	return
}
