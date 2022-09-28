package lrdd

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func BenchmarkBytesCopy(b *testing.B) {
	benchs := []struct {
		Name string
		Func func(src, dst *[]byte, srcLen int)
	}{
		{
			Name: "copy",
			Func: func(src, dst *[]byte, srcLen int) {
				copy(*dst, *src)
			},
		},
		{
			Name: "copy_partial",
			Func: func(src, dst *[]byte, srcLen int) {
				copy(*dst, (*src)[0:srcLen-1])
			},
		},
		{
			Name: "append",
			Func: func(src, dst *[]byte, srcLen int) {
				*dst = append((*dst)[:0], *src...)
			},
		},
		{
			Name: "append_partial",
			Func: func(src, dst *[]byte, srcLen int) {
				*dst = append((*dst)[:0], (*src)[0:srcLen-1]...)
			},
		},
	}

	srcLens := []int{1, 1024}

	for _, bench := range benchs {
		for _, srcLen := range srcLens {
			b.Run(fmt.Sprintf("%v-%v", bench.Name, srcLen), func(b *testing.B) {
				src := []byte(strings.Repeat("*", srcLen))

				var dsts []*[]byte
				for i := 0; i < b.N; i++ {
					dst := make([]byte, 0, srcLen)
					dsts = append(dsts, &dst)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, dst := range dsts {
						bench.Func(&src, dst, srcLen)
					}
				}
			})
		}
	}
}

func BenchmarkRow_Marshal(b *testing.B) {
	originRow := &Row{
		Key:   "foo",
		Value: []byte("bar"),
	}

	for i := 0; i < b.N; i++ {
		bs, err := proto.Marshal(originRow)
		if err != nil {
			b.Fatal(err)
		}

		var row Row

		err = proto.Unmarshal(bs, &row)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRow_MarshalVT(b *testing.B) {
	originRow := &Row{
		Key:   "foo",
		Value: []byte("bar"),
	}

	for i := 0; i < b.N; i++ {
		bs, err := originRow.MarshalVT()
		if err != nil {
			b.Fatal(err)
		}

		var row Row

		err = row.UnmarshalVT(bs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestMarshalVT(t *testing.T) {
	originRow := &Row{
		Key:   "foo",
		Value: []byte("bar"),
	}

	bs, err := originRow.MarshalVT()
	if err != nil {
		t.Fatal(err)
	}

	var row Row
	err = row.UnmarshalVT(bs)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, originRow.String(), row.String())
}

type testStruct struct {
	Foo float64
	Bar string
}
