package testutils

import (
	"strconv"

	"github.com/ab180/lrmr/lrdd"
)

func StringValue(row *lrdd.Row) string {
	return string(row.Value)
}

func IntValue(row *lrdd.Row) int {
	n, err := strconv.Atoi(string(row.Value))
	if err != nil {
		panic(err)
	}
	return n
}

func StringValues(rows []*lrdd.Row) (ss []string) {
	for _, row := range rows {
		ss = append(ss, StringValue(row))
	}
	return
}

func GroupRowsByKey(rows <-chan *lrdd.Row) map[string][]*lrdd.Row {
	grouped := make(map[string][]*lrdd.Row)
	for row := range rows {
		grouped[row.Key] = append(grouped[row.Key], row)
	}
	return grouped
}
