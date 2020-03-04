package logutils

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestWrapRecover(t *testing.T) {
	Convey("When Recover is called", t, func() {
		Convey("It should not panic", func() {
			So(func() {
				defer func() {
					if err := WrapRecover(recover()); err != nil {
						fmt.Println(err.Pretty())
					}
				}()
				panicStation()
			}, ShouldNotPanic)
		})
	})
}

func panicStation() {
	var empty map[string]string
	empty["a"] = "b"
}
