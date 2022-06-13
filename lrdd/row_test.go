package lrdd

import (
	"math"
	"testing"

	"github.com/gogo/protobuf/proto"
	. "github.com/smartystreets/goconvey/convey"
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

type testStruct struct {
	Foo float64
	Bar string
}
