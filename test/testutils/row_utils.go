package testutils

import (
	"github.com/ab180/lrmr/lrdd"
)

func StringValue(row lrdd.Row) string {
	return string(*row.Value.(*lrdd.Bytes))
}

func IntValue(row lrdd.Row) int {
	return int(*row.Value.(*lrdd.Uint64))
}

func StringValues(rows []lrdd.Row) (ss []string) {
	for _, row := range rows {
		ss = append(ss, StringValue(row))
	}
	return
}

func GroupRowsByKey(rows <-chan lrdd.Row) map[string][]lrdd.Row {
	grouped := make(map[string][]lrdd.Row)
	for row := range rows {
		grouped[row.Key] = append(grouped[row.Key], row)
	}
	return grouped
}
