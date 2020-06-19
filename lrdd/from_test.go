package lrdd

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestFrom(t *testing.T) {
	Convey("Given lrdd.From function", t, func() {
		Convey("When calling with single value input", func() {
			Convey("It should create a row when it is string", func() {
				rows := From("1234")
				So(rows, ShouldHaveLength, 1)
				So(rows[0].Key, ShouldBeEmpty)

				actual := ""
				So(func() { rows[0].UnmarshalValue(&actual) }, ShouldNotPanic)
				So(actual, ShouldEqual, "1234")
			})

			Convey("It should create a row when it is number", func() {
				rows := From(1234)
				So(rows, ShouldHaveLength, 1)
				So(rows[0].Key, ShouldBeEmpty)

				actual := 0
				So(func() { rows[0].UnmarshalValue(&actual) }, ShouldNotPanic)
				So(actual, ShouldEqual, 1234)
			})
		})

		Convey("When calling with array", func() {
			Convey("It should create a row when it is string slice", func() {
				rows := From([]string{"1", "2", "3", "4"})
				So(rows, ShouldHaveLength, 4)
				So(rows[0].Key, ShouldBeEmpty)

				actual := ""
				So(func() { rows[0].UnmarshalValue(&actual) }, ShouldNotPanic)
				So(actual, ShouldEqual, "1")
			})

			Convey("It should create a row when it is number array", func() {
				rows := From([4]int{1, 2, 3, 4})
				So(rows, ShouldHaveLength, 4)
				So(rows[0].Key, ShouldBeEmpty)

				actual := 0
				So(func() { rows[0].UnmarshalValue(&actual) }, ShouldNotPanic)
				So(actual, ShouldEqual, 1)
			})
		})

		Convey("When calling with an array of lrdd.Row", func() {
			Convey("It should not create a new row", func() {
				original := []*Row{KeyValue("hi", "ho"), Value("yo")}
				rows := From(original)
				So(original, ShouldResemble, rows)
			})
		})

		Convey("When calling with map", func() {
			Convey("It should create a row when it is string map", func() {
				rows := From(map[string]string{
					"foo": "goo",
					"bar": "baz",
				})
				So(rows, ShouldHaveLength, 2)
				count := 0
				for _, row := range rows {
					if row.Key == "foo" {
						var actual string
						So(func() { row.UnmarshalValue(&actual) }, ShouldNotPanic)
						So(actual, ShouldEqual, "goo")
						count += 1
					}
					if row.Key == "bar" {
						var actual string
						So(func() { row.UnmarshalValue(&actual) }, ShouldNotPanic)
						So(actual, ShouldEqual, "baz")
						count += 1
					}
				}
				So(count, ShouldEqual, 2)
			})

			Convey("It should create a row when it is interface map", func() {
				rows := From(map[string]interface{}{
					"foo": "goo",
					"bar": 1234,
				})
				So(rows, ShouldHaveLength, 2)

				count := 0
				for _, row := range rows {
					if row.Key == "foo" {
						var actual string
						So(func() { row.UnmarshalValue(&actual) }, ShouldNotPanic)
						So(actual, ShouldEqual, "goo")
						count += 1
					}
					if row.Key == "bar" {
						var actual int
						So(func() { row.UnmarshalValue(&actual) }, ShouldNotPanic)
						So(actual, ShouldEqual, 1234)
						count += 1
					}
				}
				So(count, ShouldEqual, 2)
			})
		})

		Convey("When calling with map containing array", func() {
			Convey("It should create a row when it is string array", func() {
				rows := From(map[string][]string{
					"foo": {"goo", "hoo"},
					"bar": {"baz"},
				})
				So(rows, ShouldHaveLength, 3)
				count := 0
				for _, row := range rows {
					if row.Key == "foo" {
						var actual string
						So(func() { row.UnmarshalValue(&actual) }, ShouldNotPanic)
						So(actual, ShouldBeIn, []string{"goo", "hoo"})
						count += 1
					}
					if row.Key == "bar" {
						var actual string
						So(func() { row.UnmarshalValue(&actual) }, ShouldNotPanic)
						So(actual, ShouldEqual, "baz")
						count += 1
					}
				}
				So(count, ShouldEqual, 3)
			})
		})
	})
}
