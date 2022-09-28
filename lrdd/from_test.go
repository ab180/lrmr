package lrdd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDuration(t *testing.T) {
	tcs := []struct {
		name string
		from time.Duration
		to   *Row
	}{
		{
			name: "second",
			from: time.Second,
			to: &Row{
				Value: []byte{0x0, 0xca, 0x9a, 0x3b, 0x0, 0x0, 0x0, 0x0},
			},
		},
		{
			name: "minute",
			from: time.Minute,
			to: &Row{
				Value: []byte{0x0, 0x58, 0x47, 0xf8, 0xd, 0x0, 0x0, 0x0},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(
			tc.name,
			func(t *testing.T) {
				rows := FromDurations(tc.from)
				require.Equal(t, 1, len(rows))

				row := rows[0]

				require.Equal(t, tc.to.Value, row.Value)

				dur := ToDuration(row)
				require.Equal(t, tc.from, dur)
			},
		)
	}
}
