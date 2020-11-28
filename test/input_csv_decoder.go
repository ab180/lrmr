package test

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
	"github.com/pkg/errors"
)

type csvDecoder struct{}

func DecodeCSV() lrmr.FlatMapper {
	return &csvDecoder{}
}

func (l *csvDecoder) FlatMap(ctx lrmr.Context, in *lrdd.Row) (result []*lrdd.Row, err error) {
	var path string
	in.UnmarshalValue(&path)

	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open file")
	}
	ctx.AddMetric("Files", 1)

	r := bufio.NewReader(file)
	csvReader := csv.NewReader(r)
	csvReader.ReuseRecord = true

	// read header
	header, err := csvReader.Read()
	if err != nil {
		return nil, errors.Wrap(err, "read csv header")
	}
	columnIndices := make(map[int]string, len(header))
	for index, columnName := range header {
		columnIndices[index] = columnName
	}

	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		msg := make(map[string]interface{}, len(row))
		for colIdx, colName := range columnIndices {
			msg[colName] = row[colIdx]
		}
		result = append(result, lrdd.KeyValue(msg["appID"].(string), msg))
	}
	return result, file.Close()
}
