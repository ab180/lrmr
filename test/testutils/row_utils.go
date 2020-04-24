package testutils

import "github.com/therne/lrmr/lrdd"

func StringValue(row *lrdd.Row) (s string) {
	row.UnmarshalValue(&s)
	return
}

func IntValue(row *lrdd.Row) (n int) {
	row.UnmarshalValue(&n)
	return
}
