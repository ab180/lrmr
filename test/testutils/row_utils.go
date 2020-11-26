package testutils

import "github.com/ab180/lrmr/lrdd"

func StringValue(row *lrdd.Row) (s string) {
	row.UnmarshalValue(&s)
	return
}

func IntValue(row *lrdd.Row) (n int) {
	row.UnmarshalValue(&n)
	return
}

func StringValues(rows []*lrdd.Row) (ss []string) {
	for _, row := range rows {
		ss = append(ss, StringValue(row))
	}
	return
}

func GroupRowsByKey(rows []*lrdd.Row) map[string][]*lrdd.Row {
	grouped := make(map[string][]*lrdd.Row)
	for _, row := range rows {
		grouped[row.Key] = append(grouped[row.Key], row)
	}
	return grouped
}
