package test

import (
	"github.com/therne/lrmr"
)

func ComplicatedQuery(sess *lrmr.Session) *lrmr.Dataset {
	return sess.FromFile("/Users/vista/testdata/").
		FlatMap(DecodeJSON()).
		Map(NopMapper()).
		GroupByKey().
		Reduce(Count())
}
