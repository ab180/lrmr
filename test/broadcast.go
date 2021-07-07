package test

import (
	"fmt"
	"log"
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

var _ = lrmr.RegisterTypes(&BroadcastStage{})

func BroadcastTester(sess *lrmr.Session) *lrmr.Dataset {
	return sess.Parallelize("dummy").
		Broadcast("ThroughContext", "bar").
		Broadcast("AnyType", time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC)).
		Map(&BroadcastStage{ThroughStruct: "foo"})
}

type BroadcastStage struct {
	ThroughStruct string
}

func (b *BroadcastStage) Map(c lrmr.Context, row *lrdd.Row) (*lrdd.Row, error) {
	v := c.Broadcast("ThroughContext")
	typeMatched := c.Broadcast("AnyType").(time.Time) == time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC)

	output := fmt.Sprintf("throughStruct=%s, throughContext=%v, typeMatched=%v", b.ThroughStruct, v, typeMatched)
	log.Println("output is ", output)
	return lrdd.Value(output), nil
}
