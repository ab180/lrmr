package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/test/testdata"
)

func ComplicatedQuery() *lrmr.Pipeline {
	return lrmr.FromLocalFile(testdata.Path()).
		FlatMap(DecodeCSV()).
		Map(NopMapper()).
		GroupByKey().
		Reduce(Count())
}
