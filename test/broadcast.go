package test

import (
	"fmt"
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/stage"
)

func BroadcastTester(m *master.Master) *lrmr.Dataset {
	return lrmr.Parallelize("dummy", m).
		Broadcast("ThroughContext", "bar").
		Map(&BroadcastStage{ThroughStruct: "foo"})
}

var _ = stage.RegisterMap("BroadcastStage", &BroadcastStage{})

type BroadcastStage struct {
	ThroughStruct string
}

func (b *BroadcastStage) Map(c stage.Context, row *lrdd.Row) (*lrdd.Row, error) {
	v := c.Broadcast("ThroughContext")
	return lrdd.Value(fmt.Sprintf("throughStruct=%s, throughContext=%v", b.ThroughStruct, v)), nil
}
