package test

import (
	"github.com/therne/lrmr"
	. "github.com/therne/lrmr/playground"
)

func BasicGroupByKey(m *lrmr.Master) *lrmr.Dataset {
	return lrmr.FromURI("/Users/vista/testdata/", m).
		FlatMap(DecodeJSON()).
		GroupByKey().
		Reduce(Count())
}

func BasicGroupByKnownKeys(m *lrmr.Master) *lrmr.Dataset {
	return lrmr.FromURI("/Users/vista/testdata/", m).
		FlatMap(DecodeJSON()).
		GroupByKnownKeys([]string{"1737", "777", "1364", "6038"}).
		Reduce(Count())
}
