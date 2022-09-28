package test

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
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
	path := string(in.Value)

	file, err := os.Open(path) // #nosec G304, We should check safe path with filepath.Clean
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

		bs, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal json: %w", err)
		}

		result = append(result, &lrdd.Row{Key: msg["appID"].(string), Value: bs})
	}
	return result, file.Close()
}
