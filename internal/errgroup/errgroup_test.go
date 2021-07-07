package errgroup

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGroup_Go(t *testing.T) {
	Convey("Using errgroup.Go", t, func() {
		wg, _ := WithContext(context.TODO())

		Convey("It should recover panic as error", func() {
			for i := 0; i < 100; i++ {
				wg.Go(func() error {
					panic("hi")
				})
			}
			So(wg.Wait(), ShouldNotBeNil)
		})
	})
}
