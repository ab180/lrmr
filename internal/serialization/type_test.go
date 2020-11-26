package serialization

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type AliasInt int

func TestTypeFromString(t *testing.T) {
	Convey("Calling serialization.TypeFromString", t, func() {

		Convey("With primitives", func() {
			Convey("It should not raise any error", func() {
				t, err := TypeFromString("int")
				So(err, ShouldBeNil)
				So(t.IsSameType(0), ShouldBeTrue)

				t, err = TypeFromString("string")
				So(err, ShouldBeNil)
				So(t.IsSameType("string"), ShouldBeTrue)

				t, err = TypeFromString("float32")
				So(err, ShouldBeNil)
				So(t.IsSameType(float32(0.0)), ShouldBeTrue)
			})
		})

		Convey("With wrapped primitives", func() {
			Convey("It should not raise any error", func() {
				t, err := TypeFromString("[]int")
				So(err, ShouldBeNil)
				So(t.IsSameType([]int{0}), ShouldBeTrue)

				t, err = TypeFromString("[]string")
				So(err, ShouldBeNil)
				So(t.IsSameType([]string{"string"}), ShouldBeTrue)

				t, err = TypeFromString("[]float32")
				So(err, ShouldBeNil)
				So(t.IsSameType([]float32{0.0}), ShouldBeTrue)
			})
		})

		Convey("With alias of primitives", func() {
			tt := TypeOf(AliasInt(0))
			So(tt.String(), ShouldEqual, "github.com/ab180/lrmr/internal/serialization.AliasInt")

			Convey("It should not raise any error", func() {
				t, err := TypeFromString("[]github.com/ab180/lrmr/internal/serialization.AliasInt")
				So(err, ShouldBeNil)
				So(t.IsSameType([]AliasInt{0}), ShouldBeTrue)
			})
		})
	})
}
