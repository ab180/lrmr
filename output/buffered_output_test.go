package output

import (
	"github.com/ab180/lrmr/lrdd"
	. "github.com/smartystreets/goconvey/convey"
	"strconv"
	"testing"
)

const bufSize = 10

func TestNewBufferedOutput(t *testing.T) {
	Convey("Given NewBufferedOutput", t, func() {
		Convey("When calling it with zero buffer size", func() {
			Convey("It should panic", func() {
				So(func() { NewBufferedOutput(nil, 0) }, ShouldPanic)
			})
		})
	})
}

func TestBufferedOutput_Write(t *testing.T) {
	Convey("Calling Write to BufferedOutput", t, func() {
		m := &outputMock{}
		o := NewBufferedOutput(m, bufSize)

		Convey("When writing items shorter than the buffer size to the buffer", func() {
			it := items(bufSize / 2)
			err := o.Write(it...)
			So(err, ShouldBeNil)

			Convey("It should not write to the original output", func() {
				So(m.Rows, ShouldBeEmpty)
			})

			Convey("When calling flush", func() {
				err := o.Flush()
				So(err, ShouldBeNil)

				Convey("It should write to the original output", func() {
					So(m.Rows, ShouldResemble, it)
				})
			})
		})

		Convey("When writing items larger than the buffer size to the buffer", func() {
			err := o.Write(items(bufSize * 5)...)
			So(err, ShouldBeNil)

			Convey("It should flush multiple times", func() {
				So(m.Calls.Write, ShouldEqual, 5)
			})
		})

		Convey("When writing items wrapped to the buffer size", func() {
			var it []*lrdd.Row

			So(o.Write(items(bufSize/2)...), ShouldBeNil)
			it = append(it, items(bufSize/2)...)

			So(o.Write(items(bufSize)...), ShouldBeNil)
			it = append(it, items(bufSize)...)

			Convey("It should flush before the wrap point", func() {
				So(m.Calls.Write, ShouldEqual, 1)
				So(m.Rows, ShouldResemble, it[:bufSize])
			})

			Convey("When calling flush", func() {
				err := o.Flush()
				So(err, ShouldBeNil)

				Convey("It should write rest of the items", func() {
					So(m.Calls.Write, ShouldEqual, 2)
					So(m.Rows, ShouldResemble, it)
				})
			})
		})
	})
}

func TestBufferedOutput_Flush(t *testing.T) {
	Convey("Calling Flush to BufferedOutput", t, func() {
		m := &outputMock{}
		o := NewBufferedOutput(m, bufSize)

		Convey("When there are items", func() {
			it := items(bufSize / 2)
			So(o.Write(it...), ShouldBeNil)

			Convey("It should write them to the original output", func() {
				So(o.Flush(), ShouldBeNil)
				So(m.Rows, ShouldResemble, it)
			})
		})

		Convey("After flushed all items", func() {
			So(o.Write(items(bufSize/2)...), ShouldBeNil)
			So(o.Flush(), ShouldBeNil)
			So(m.Rows, ShouldHaveLength, bufSize/2)

			Convey("It should not write anything with no error", func() {
				So(o.Flush(), ShouldBeNil)
				So(m.Rows, ShouldHaveLength, bufSize/2)
			})
		})
	})
}

func items(length int) (rr []*lrdd.Row) {
	for i := 0; i < length; i++ {
		rr = append(rr, lrdd.Value(strconv.Itoa(i)))
	}
	return
}
