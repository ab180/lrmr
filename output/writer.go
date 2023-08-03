package output

import (
	"errors"
	"fmt"

	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/partitions"
)

type Writer struct {
	context     partitions.Context
	partitioner partitions.Partitioner
	isPreserved bool

	// outputs is a mapping of partition ID to an output.
	outputs map[string]Output
}

func NewWriter(partitionID string, p partitions.Partitioner, outputs map[string]Output) *Writer {
	return &Writer{
		context:     partitions.NewContext(partitionID),
		partitioner: p,
		isPreserved: partitions.IsPreserved(p),
		outputs:     outputs,
	}
}

func (w *Writer) Write(data []lrdd.Row) error {
	if w.isPreserved {
		output := w.outputs[w.context.PartitionID()]
		if output == nil {
			// probably the last stage
			return nil
		}
		return output.Write(data)
	}
	// TODO: reuse
	writes := make(map[string][]lrdd.Row)
	for _, row := range data {
		id, err := w.partitioner.DeterminePartition(w.context, row, len(w.outputs))
		if err != nil {
			if err == partitions.ErrNoOutput {
				// TODO: add alert if too many outputs are skipped
				continue
			}
			return err
		}
		writes[id] = append(writes[id], row)
	}
	for id, rows := range writes {
		out, ok := w.outputs[id]
		if !ok {
			return fmt.Errorf("unknown partition ID %s", id)
		}
		if err := out.Write(rows); err != nil {
			return fmt.Errorf("%w write %d rows to partition %s", err, len(rows), id)
		}
	}
	return nil
}

func (w *Writer) Dispatch(taskID string, n int) ([]lrdd.Row, error) {
	o, ok := w.outputs[taskID]
	if !ok {
		return nil, fmt.Errorf("unknown task %v", taskID)
	}
	if p, ok := o.(PullStream); ok {
		return p.Dispatch(n), nil
	}
	return nil, nil
}

func (w Writer) NumOutputs() int {
	return len(w.outputs)
}

func (w *Writer) Close() error {
	var errs error
	for k, out := range w.outputs {
		if err := out.Close(); err != nil {
			errs = errors.Join(errs, err)
		}
		delete(w.outputs, k)
	}
	w.outputs = nil
	return errs
}
