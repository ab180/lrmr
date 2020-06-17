package serialization

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/node"
)

func TestSerializeStruct(t *testing.T) {
	Convey("Calling SerializeStruct", t, func() {

		Convey("On a plain struct", func() {
			expected := node.Node{
				ID:   "hello",
				Host: "world",
				Type: "foo",
			}
			actual := serializeAndDeserialize(expected)
			Convey("It should be same after serialization", func() {
				So(actual, ShouldResemble, expected)
			})
		})

		Convey("On a struct pointer", func() {
			expected := &node.Node{
				ID:   "hello",
				Host: "world",
				Type: "foo",
			}
			actual := serializeAndDeserialize(expected)
			Convey("It should be same after serialization", func() {
				So(actual, ShouldResemble, expected)
			})
		})

		Convey("On a struct slice", func() {
			expected := []node.Node{
				{ID: "fun", Host: "hello"},
				{ID: "sad", Host: "world"},
			}
			actual := serializeAndDeserialize(expected)
			Convey("It should be same after serialization", func() {
				So(actual, ShouldResemble, expected)
			})
		})

		Convey("On a struct pointer slice", func() {
			expected := []*node.Node{
				{ID: "fun", Host: "hello"},
				{ID: "sad", Host: "world"},
			}
			actual := serializeAndDeserialize(expected)
			Convey("It should be same after serialization", func() {
				So(actual, ShouldResemble, expected)
			})
		})

		Convey("On empty field struct", func() {
			expected := empty{}
			actual := serializeAndDeserialize(expected)

			Convey("It should same after serialization", func() {
				So(actual, ShouldResemble, expected)
			})
		})

		Convey("On a struct pointer with nil", func() {
			var expected *node.Node
			actual := serializeAndDeserialize(expected)

			Convey("It should same after serialization", func() {
				So(actual, ShouldResemble, expected)
			})
		})

		Convey("On a primitive", func() {
			expected := 3
			Convey("It should raise error on serialization", func() {
				So(func() { serializeAndDeserialize(expected) }, ShouldPanic)
			})
		})
	})
}

func serializeAndDeserialize(v interface{}) interface{} {
	s, err := SerializeStruct(v)
	if err != nil {
		panic(err)
	}
	vv, err := DeserializeStruct(s)
	if err != nil {
		panic(err)
	}
	return vv
}

type empty struct{}
