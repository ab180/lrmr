package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/test/testdata"
)

func BasicGroupByKey() *lrmr.Pipeline {
	return lrmr.FromLocalFile(testdata.Path()).
		FlatMap(DecodeCSV()).
		GroupByKey().
		Reduce(Count())
}

func BasicGroupByKnownKeys() *lrmr.Pipeline {
	return lrmr.FromLocalFile(testdata.Path()).
		FlatMap(DecodeCSV()).
		GroupByKnownKeys([]string{"8263", "9223", "8636", "3962"}).
		Reduce(Count())
}

func SimpleCount() *lrmr.Pipeline {
	d := map[string][]string{
		"foo": {"goo", "hoo"},
		"bar": {"baz"},
	}
	return lrmr.Parallelize(d).
		GroupByKey().
		Reduce(Count())
}
