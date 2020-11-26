package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/test/testdata"
)

func BasicGroupByKey(sess *lrmr.Session) *lrmr.Dataset {
	return sess.FromFile(testdata.Path()).
		FlatMap(DecodeJSON()).
		GroupByKey().
		Reduce(Count())
}

func BasicGroupByKnownKeys(sess *lrmr.Session) *lrmr.Dataset {
	return sess.FromFile(testdata.Path()).
		FlatMap(DecodeJSON()).
		GroupByKnownKeys([]string{"1737", "777", "1364", "6038"}).
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
