package lrdd

import (
	"github.com/gogo/protobuf/proto"
	. "github.com/smartystreets/goconvey/convey"
	"math"
	"testing"
)

func TestRow_Encode(t *testing.T) {
	Convey("When encoding a row", t, func() {
		var row Row
		Convey("With int", func() {
			var decoded int
			v := 1234
			raw, _ := Value(v).Marshal()

			Convey("Its type should be preserved", func() {
				err := proto.Unmarshal(raw, &row)
				So(err, ShouldBeNil)
				row.UnmarshalValue(&decoded)
				So(decoded, ShouldEqual, v)
			})
		})

		Convey("With int64", func() {
			var decoded int64
			v := int64(1234)
			raw, _ := Value(v).Marshal()

			Convey("Its type should be preserved", func() {
				err := proto.Unmarshal(raw, &row)
				So(err, ShouldBeNil)
				row.UnmarshalValue(&decoded)
				So(decoded, ShouldEqual, v)
			})
		})

		Convey("With float64", func() {
			var decoded float64
			v := float64(math.Pi)
			raw, _ := Value(v).Marshal()

			Convey("Its type should be preserved", func() {
				err := proto.Unmarshal(raw, &row)
				So(err, ShouldBeNil)
				row.UnmarshalValue(&decoded)
				So(decoded, ShouldEqual, v)
			})
		})

		Convey("With struct", func() {
			var decoded testStruct
			v := &testStruct{Foo: math.Pi, Bar: "good"}
			raw, _ := Value(v).Marshal()

			Convey("Its type should be preserved", func() {
				err := proto.Unmarshal(raw, &row)
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
