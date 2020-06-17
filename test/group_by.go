package test

import (
	"github.com/therne/lrmr"
	. "github.com/therne/lrmr/playground"
)

func BasicGroupByKey(sess *lrmr.Session) *lrmr.Dataset {
	return sess.FromFile("/Users/vista/testdata/").
		FlatMap(DecodeJSON()).
		GroupByKey().
		Reduce(Count())
}

func BasicGroupByKnownKeys(sess *lrmr.Session) *lrmr.Dataset {
	return sess.FromFile("/Users/vista/testdata/").
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
