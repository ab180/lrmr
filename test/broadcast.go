package test

import (
	"fmt"
	"log"
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

var _ = lrmr.RegisterTypes(&BroadcastStage{})

func BroadcastTester() *lrmr.Pipeline {
	return lrmr.Parallelize(lrdd.FromStrings("dummy")).
		Broadcast("ThroughContext", "bar").
		Broadcast("AnyType", time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC)).
		Map(&BroadcastStage{ThroughStruct: "foo"})
}

type BroadcastStage struct {
	ThroughStruct string
}

func (b *BroadcastStage) Map(c lrmr.Context, rows []lrdd.Row) ([]lrdd.Row, error) {
	mappedRows := make([]lrdd.Row, len(rows))
	for i := range rows {
		v := c.Broadcast("ThroughContext")
		typeMatched := c.Broadcast("AnyType").(time.Time) == time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC)

		output := fmt.Sprintf("throughStruct=%s, throughContext=%v, typeMatched=%v", b.ThroughStruct, v, typeMatched)
		log.Println("output is ", output)

		mappedRows[i] = lrdd.Row{Value: lrdd.NewBytes(output)}
	}

	return mappedRows, nil
}

func (b *BroadcastStage) RowType() lrdd.RowType {
	return lrdd.RowTypeBytes
}
