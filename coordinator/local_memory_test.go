package coordinator

import (
	gocontext "context"
	"sort"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
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

			keys := []string{items[0].Key, items[1].Key, items[2].Key}
			sort.Strings(keys)
			So(keys, ShouldResemble, []string{"testKey", "testKey1", "testKey2"})
		})
	})
}

func TestLocalMemoryCoordinator_GrantLease(t *testing.T) {
	Convey("Given LocalMemoryCoordinator", t, func() {
		crd := NewLocalMemory()
		ctx := gocontext.Background()

		l, err := crd.GrantLease(ctx, 200*time.Millisecond)
		So(err, ShouldBeNil)

		So(crd.Put(ctx, "testKey1", "testValue1", WithLease(l)), ShouldBeNil)
		So(crd.Put(ctx, "testKey2", "testValue1", WithLease(l)), ShouldBeNil)

		Convey("It should be retrieved within TTL", func() {
			items, err := crd.Scan(ctx, "testKey")
			So(err, ShouldBeNil)

			So(items, ShouldHaveLength, 2)
			So(items[0].Key, ShouldEqual, "testKey1")
			So(items[1].Key, ShouldEqual, "testKey2")
		})

		Convey("It should be deleted after TTL", func() {
			time.Sleep(250 * time.Millisecond)
			items, err := crd.Scan(ctx, "testKey")
			So(err, ShouldBeNil)
			So(items, ShouldHaveLength, 0)
		})
	})
}
