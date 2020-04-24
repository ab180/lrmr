package test

import (
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/master"
	. "github.com/therne/lrmr/playground"
)

func BasicGroupByKey(m *master.Master) *lrmr.Dataset {
	return lrmr.FromURI("/Users/vista/testdata/", m).
		FlatMap(DecodeJSON()).
		GroupByKey().
		Reduce(Count())
}

func BasicGroupByKnownKeys(m *master.Master) *lrmr.Dataset {
	return lrmr.FromURI("/Users/vista/testdata/", m).
		FlatMap(DecodeJSON()).
		GroupByKnownKeys([]string{"1737", "777", "1364", "6038"}).
		Reduce(Count())
}

func SimpleCount(m *master.Master) *lrmr.Dataset {
	d := map[string][]string{
		"foo": {"goo", "hoo"},
		"bar": {"baz"},
	}
	return lrmr.Parallelize(d, m).
		GroupByKey().
		Reduce(Count())
}
