package coordinator

import (
	gocontext "context"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestLocalMemoryCoordinator_Get(t *testing.T) {
	Convey("Given LocalMemoryCoordinator", t, func() {
		crd := NewLocalMemory()
		ctx := gocontext.Background()
		So(crd.Put(ctx, "testKey", "testValue"), ShouldBeNil)

		Convey("It should retrieve item using Get", func() {
			var val string
			err := crd.Get(ctx, "testKey", &val)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "testValue")
		})
	})
}

func TestLocalMemoryCoordinator_Scan(t *testing.T) {
	Convey("Given LocalMemoryCoordinator", t, func() {
		crd := NewLocalMemory()
		ctx := gocontext.Background()
		So(crd.Put(ctx, "testKey", "testValue"), ShouldBeNil)
		So(crd.Put(ctx, "testKey1", "testValue"), ShouldBeNil)
		So(crd.Put(ctx, "testKey2", "testValue"), ShouldBeNil)
		So(crd.Put(ctx, "jestKey1", "testValue1"), ShouldBeNil)

		Convey("It should retrieve items using Scan", func() {
			items, err := crd.Scan(ctx, "testKey")
			So(err, ShouldBeNil)

			So(items, ShouldHaveLength, 3)
			So(items[0].Key, ShouldEqual, "testKey")
			So(items[1].Key, ShouldEqual, "testKey1")
			So(items[2].Key, ShouldEqual, "testKey2")
		})
	})
}
