package lrmrmetric

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMetrics_String(t *testing.T) {
	m := Metrics{
		"a": 1,
		"b": 2,
		"c": 3,
	}

	require.Equal(
		t,
		m.String(),
		` - a: 1
 - b: 2
 - c: 3
`,
	)
}
