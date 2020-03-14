package playground

import (
	"bufio"
	"bytes"
	"github.com/airbloc/logger"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/stage"
	"io"
	"os"
	"strconv"
)

var _ = stage.RegisterFlatMap("DecodeJSON", DecodeJSON())

type jsonDecoder struct{}

func DecodeJSON() stage.FlatMapper {
	return &jsonDecoder{}
}

func (l *jsonDecoder) FlatMap(c stage.Context, in *lrdd.Row) (result []*lrdd.Row, err error) {
	var path string
	in.UnmarshalValue(&path)

	logger.New("jsondecoder").Verbose("Opening {}", path)

	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open file")
	}
	c.AddMetric("Files", 1)

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
		data := msg["data"].(map[string]interface{})
		app := data["app"].(map[string]interface{})
		appID := strconv.Itoa(int(app["appID"].(float64)))
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
