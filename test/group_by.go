package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/test/testdata"
)

func BasicGroupByKey(sess *lrmr.Session) *lrmr.Dataset {
	return sess.FromFile(testdata.Path()).
		FlatMap(DecodeCSV()).
		GroupByKey().
		Reduce(Count())
}

func BasicGroupByKnownKeys(sess *lrmr.Session) *lrmr.Dataset {
	return sess.FromFile(testdata.Path()).
		FlatMap(DecodeCSV()).
		GroupByKnownKeys([]string{"8263", "9223", "8636", "3962"}).
		Reduce(Count())
}

func SimpleCount(sess *lrmr.Session) *lrmr.Dataset {
	d := map[string][]string{
		"foo": {"goo", "hoo"},
		"bar": {"baz"},
	}
	return sess.Parallelize(d).
		GroupByKey().
		Reduce(Count())
}
