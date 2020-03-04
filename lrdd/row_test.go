package lrdd

import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/vmihailenco/msgpack"
	"testing"
)

func TestRow_Marshal(t *testing.T) {
	Convey("Given a row", t, func() {
		r := Row{
			"foo": "bar",
			"goo": []string{"nono", "dede"},
		}
		Convey("When marshalling it", func() {
			data := r.Marshal()
			decoded := make(Row)
			So(msgpack.Unmarshal(data, &decoded), ShouldBeNil)

			Convey("Its type should be preserved", func() {
				So(decoded["foo"], ShouldHaveSameTypeAs, "bar")
				So(decoded["foo"], ShouldEqual, "bar")
				So(decoded["goo"], ShouldHaveSameTypeAs, []string{"dede"})
				So(decoded["goo"], ShouldEqual, []string{"nono", "dede"})
			})
		})
	})
}
