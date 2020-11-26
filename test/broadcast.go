package test

import (
	"fmt"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

var _ = lrmr.RegisterTypes(&BroadcastStage{})

func BroadcastTester(sess *lrmr.Session) *lrmr.Dataset {
	return sess.Parallelize("dummy").
		Broadcast("ThroughContext", "bar").
		Map(&BroadcastStage{ThroughStruct: "foo"})
}

type BroadcastStage struct {
	ThroughStruct string
}

func (b *BroadcastStage) Map(c lrmr.Context, row *lrdd.Row) (*lrdd.Row, error) {
	v := c.Broadcast("ThroughContext")
	return lrdd.Value(fmt.Sprintf("throughStruct=%s, throughContext=%v", b.ThroughStruct, v)), nil
}
