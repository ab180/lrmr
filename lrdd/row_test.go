package lrdd

import (
	"math"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

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

func TestRow_Encode(t *testing.T) {
	Convey("When encoding a row", t, func() {
		row := new(Row)
		Convey("With int", func() {
			var decoded int
			v := 1234
			raw, _ := proto.Marshal(Value(v))

			Convey("Its type should be preserved", func() {
				err := proto.Unmarshal(raw, row)
				So(err, ShouldBeNil)
				row.UnmarshalValue(&decoded)
				So(decoded, ShouldEqual, v)
			})
		})

		Convey("With int64", func() {
			var decoded int64
			v := int64(1234)
			raw, _ := proto.Marshal(Value(v))

			Convey("Its type should be preserved", func() {
				err := proto.Unmarshal(raw, row)
				So(err, ShouldBeNil)
				row.UnmarshalValue(&decoded)
				So(decoded, ShouldEqual, v)
			})
		})

		Convey("With float64", func() {
			var decoded float64
			v := float64(math.Pi)
			raw, _ := proto.Marshal(Value(v))

			Convey("Its type should be preserved", func() {
				err := proto.Unmarshal(raw, row)
				So(err, ShouldBeNil)
				row.UnmarshalValue(&decoded)
				So(decoded, ShouldEqual, v)
			})
		})

		Convey("With struct", func() {
			var decoded testStruct
			v := &testStruct{Foo: math.Pi, Bar: "good"}
			raw, _ := proto.Marshal(Value(v))

			Convey("Its type should be preserved", func() {
				err := proto.Unmarshal(raw, row)
				So(err, ShouldBeNil)
				row.UnmarshalValue(&decoded)
				So(decoded.Foo, ShouldEqual, v.Foo)
				So(decoded.Bar, ShouldEqual, v.Bar)
			})
		})
	})
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
