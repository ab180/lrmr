package output

import "github.com/therne/lrmr/lrdd"

type outputMock struct {
	Rows []*lrdd.Row

	Calls struct {
		Write int
		Close int
	}
}

func (o *outputMock) Write(rows ...*lrdd.Row) error {
	o.Rows = append(o.Rows, rows...)
	o.Calls.Write += 1
	return nil
}

func (o *outputMock) Close() error {
	o.Calls.Close += 1
	return nil
}
