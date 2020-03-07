package playground

import (
	"bufio"
	"bytes"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
	"io"
	"os"
	"strconv"
)

var _ = stage.Register("DecodeJSON", DecodeNDJSON())

type ndjsonDecoder struct {
	stage.Simple
}

func DecodeNDJSON() stage.Runner {
	return &ndjsonDecoder{}
}

func (l *ndjsonDecoder) Apply(c stage.Context, row lrdd.Row, out output.Writer) error {
	path := row["path"].(string)

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file : %w", err)
	}
	c.AddMetric("Files", 1)

	r := bufio.NewReader(file)
	for {
		line, err := readline(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		msg := make(lrdd.Row)
		if err := jsoniter.Unmarshal(line, &msg); err != nil {
			return err
		}
		data := msg["data"].(map[string]interface{})
		app := data["app"].(map[string]interface{})
		msg["appID"] = strconv.Itoa(int(app["appID"].(float64)))
		if err := out.Write(msg); err != nil {
			return err
		}
	}
	return file.Close()
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
