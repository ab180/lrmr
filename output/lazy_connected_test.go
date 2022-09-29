package output

import (
	"testing"

	"github.com/ab180/lrmr/lrdd"
	. "github.com/smartystreets/goconvey/convey"
)

func TestLazyInitialized(t *testing.T) {
	Convey("Applying output.LazyInitialized on an output", t, func() {
		mock := &outputMock{}
		initializerCalled := false
		output := LazyInitialized(func() (Output, error) {
			initializerCalled = true
			return mock, nil
		})

		Convey("It should not initialize at the first", func() {
			So(initializerCalled, ShouldBeFalse)
		})

		Convey("It should initialize on first write", func() {
			So(initializerCalled, ShouldBeFalse)
			_ = output.Write(&lrdd.Row{Value: []byte("foo")})
			So(initializerCalled, ShouldBeTrue)
			So(mock.Calls.Write, ShouldEqual, 1)
		})
	})
}
