package coordinator

import (
	gocontext "context"
	"sort"
	"testing"
	"time"

	"github.com/samber/lo"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
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

func TestLocalMemoryCoordinator_CAS(t *testing.T) {
	Convey("Given LocalMemoryCoordinator", t, func() {
		crd := NewLocalMemory()
		ctx := gocontext.Background()

		So(crd.Put(ctx, "testKey1", "testValue1"), ShouldBeNil)

		Convey("It should put new key", func() {
			swapped, err := crd.CAS(ctx, "testKey2", nil, "testValue2")
			So(swapped, ShouldBeTrue)
			So(err, ShouldBeNil)

			var val string
			err = crd.Get(ctx, "testKey2", &val)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "testValue2")
		})

		Convey("It should update old value", func() {
			var val string
			err := crd.Get(ctx, "testKey1", &val)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "testValue1")

			swapped, err := crd.CAS(ctx, "testKey1", "testValue1", "testValue2")
			So(swapped, ShouldBeTrue)
			So(err, ShouldBeNil)

			err = crd.Get(ctx, "testKey1", &val)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "testValue2")
		})

		Convey("It should not update old value", func() {
			var val string
			err := crd.Get(ctx, "testKey1", &val)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "testValue1")

			swapped, err := crd.CAS(ctx, "testKey1", "testValue2", "testValue3")
			So(swapped, ShouldBeFalse)
			So(err, ShouldBeNil)

			err = crd.Get(ctx, "testKey1", &val)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "testValue1")
		})

		Convey("It should delete the key", func() {
			var val string
			err := crd.Get(ctx, "testKey1", &val)
			So(err, ShouldBeNil)
			So(val, ShouldEqual, "testValue1")

			swapped, err := crd.CAS(ctx, "testKey1", "testValue1", nil)
			So(swapped, ShouldBeTrue)
			So(err, ShouldBeNil)

			err = crd.Get(ctx, "testKey1", &val)
			So(err, ShouldBeError, ErrNotFound)
		})
	})
}

func TestLocalMemoryCoordinator_GrantLease_RetrievedWithinTTL(t *testing.T) {
	crd := NewLocalMemory()
	ctx := gocontext.Background()

	l, err := crd.GrantLease(ctx, 10*time.Second)
	require.Nil(t, err)

	testKeys := []string{"testKey1", "testKey2"}

	for _, testKey := range testKeys {
		err = crd.Put(ctx, testKey, "testValue", WithLease(l))
		require.Nil(t, err)
	}

	items, err := crd.Scan(ctx, "testKey")
	require.Nil(t, err)

	require.Equal(t, len(testKeys), len(items))

	actualKeys := lo.Map(items, func(t RawItem, _ int) string {
		return t.Key
	})
	sort.Strings(actualKeys)

	require.Equal(t, testKeys, actualKeys)
}

func TestLocalMemoryCoordinator_GrantLease_RetrievedAfterTTL(t *testing.T) {
	crd := NewLocalMemory()
	ctx := gocontext.Background()

	l, err := crd.GrantLease(ctx, 1*time.Millisecond)
	require.Nil(t, err)

	testKey1 := "testKey1"
	testKey2 := "testKey2"

	err = crd.Put(ctx, testKey1, "testValue1", WithLease(l))
	require.Nil(t, err)

	err = crd.Put(ctx, testKey2, "testValue1", WithLease(l))
	require.Nil(t, err)

	time.Sleep(500 * time.Millisecond)
	items, err := crd.Scan(ctx, "testKey")
	require.Nil(t, err)

	require.Equal(t, 0, len(items))
}
