package output

import (
	"github.com/ab180/lrmr/lrdd"
)

type ComposedOutput struct {
	outputs []Output
}

func NewComposed(outputs []Output) Output {
	return &ComposedOutput{outputs}
}

func (c ComposedOutput) Write(rows ...*lrdd.Row) error {
	for _, o := range c.outputs {
		if err := o.Write(rows...); err != nil {
			return err
		}
	}
	return nil
}

func (c ComposedOutput) Close() error {
	for _, o := range c.outputs {
		if err := o.Close(); err != nil {
			return err
		}
	}
	return nil
}
