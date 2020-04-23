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
