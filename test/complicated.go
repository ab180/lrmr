package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/test/testdata"
)

func ComplicatedQuery(sess *lrmr.Session) *lrmr.Dataset {
	return sess.FromFile(testdata.Path()).
		FlatMap(DecodeCSV()).
		Map(NopMapper()).
		GroupByKey().
		Reduce(Count())
}
