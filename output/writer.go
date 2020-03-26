package output

import (
	"github.com/pkg/errors"
	"github.com/therne/lrmr/lrdd"
)

type Writer struct {
	partitioner Partitioner

	// outputs is a mapping of partition key to an output.
	outputs map[string]Output
}

func NewWriter(p Partitioner, outputs map[string]Output) *Writer {
	return &Writer{
		partitioner: p,
		outputs:     outputs,
	}
}

func (w *Writer) Write(data []*lrdd.Row) error {
	writes := make(map[string][]*lrdd.Row)
	for _, row := range data {
		pk, err := w.partitioner.DeterminePartitionKey(row)
		if err != nil {
			if err == ErrNoOutput {
				continue
			}
			return err
		}
		writes[pk] = append(writes[pk], row)
	}
	for slot, rows := range writes {
		out, ok := w.outputs[slot]
		if !ok {
			return errors.Errorf("unknown partition %s", slot)
		}
		if err := out.Write(rows); err != nil {
			return errors.Wrapf(err, "write %d rows to partition %s", len(rows), slot)
		}
	}
	return nil
}

func (w *Writer) Close() error {
	for _, out := range w.outputs {
		if err := out.Close(); err != nil {
			return err
		}
	}
	return nil
}
