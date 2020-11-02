package integration

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/test/testutils"
	"github.com/thoas/go-funk"
	"golang.org/x/sync/errgroup"
)

func TestEtcd_Counter(t *testing.T) {
	RunOnIntegrationTest(t)
	Convey("Given an etcd cluster", t, WithEtcd(func(etcd coordinator.Coordinator) {
		n := 100
		var counterRecords sync.Map

		Convey("Calling count() with a race condition", func(c C) {
			wg, wctx := errgroup.WithContext(testutils.ContextWithTimeout())
			for i := 0; i < n; i++ {
				wg.Go(func() error {
					count, err := etcd.IncrementCounter(wctx, "counter")
					if err != nil {
						return err
					}
					if _, duplicated := counterRecords.LoadOrStore(count, true); duplicated {
						return fmt.Errorf("number %d is duplicated", count)
					}
					return nil
				})
			}
			err := wg.Wait()
			So(err, ShouldBeNil)

			Convey("Should increment counter correctly", func() {
				counter, err := etcd.ReadCounter(testutils.ContextWithTimeout(), "counter")
				So(err, ShouldBeNil)
				So(counter, ShouldEqual, n)

				Convey("IncrementCounter should have returned atomically increased count", func() {
					// ensure that no misses on the record
					missed := "no miss"
					for i := int64(1); i <= int64(n); i++ {
						if _, exists := counterRecords.Load(i); !exists {
							missed = fmt.Sprintf("missing number %d", i)
							break
						}
					}
					So(missed, ShouldEqual, "no miss")
				})
			})
		})
	}))
}

func TestEtcd_Transaction(t *testing.T) {
	RunOnIntegrationTest(t)
	Convey("Given an etcd cluster", t, WithEtcd(func(etcd coordinator.Coordinator) {
		n := 100
		m := 10
		var (
			duplicateCounts1 sync.Map
			duplicateCounts2 sync.Map
		)

		Convey("Calling count() within a transaction with race condition", func(c C) {
			wg, wctx := errgroup.WithContext(testutils.ContextWithTimeout())
			for i := 0; i < n; i++ {
				wg.Go(func() error {
					for j := 0; j < m; j++ {
						txnResults, err := etcd.Commit(wctx, coordinator.NewTxn().
							IncrementCounter("counter1").
							IncrementCounter("counter2"))

						if err != nil {
							return err
						}
						if _, duplicated := duplicateCounts1.LoadOrStore(txnResults[0].Counter, true); duplicated {
							return fmt.Errorf("counter1: number %d is duplicated", txnResults[0].Counter)
						}
						if _, duplicated := duplicateCounts2.LoadOrStore(txnResults[1].Counter, true); duplicated {
							return fmt.Errorf("counter2: number %d is duplicated", txnResults[1].Counter)
						}
					}
					return nil
				})
			}
			err := wg.Wait()
			So(err, ShouldBeNil)

			Convey("Should increment counter correctly", func() {
				counter, err := etcd.ReadCounter(testutils.ContextWithTimeout(), "counter1")
				So(err, ShouldBeNil)
				So(counter, ShouldEqual, n*m)

				counter, err = etcd.ReadCounter(testutils.ContextWithTimeout(), "counter2")
				So(err, ShouldBeNil)
				So(counter, ShouldEqual, n*m)
			})
		})
	}))
}

func WithEtcd(fn func(etcd coordinator.Coordinator)) func() {
	return func() {
		rand.Seed(time.Now().Unix())

		testNs := fmt.Sprintf("lrmr_test_%s/", funk.RandomString(10))
		etcd, err := coordinator.NewEtcd([]string{"127.0.0.1:2379"}, testNs)
		So(err, ShouldBeNil)

		// clean all items under test namespace
		Reset(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			_, err := etcd.Delete(ctx, "")
			So(err, ShouldBeNil)
			So(etcd.Close(), ShouldBeNil)
		})

		fn(etcd)
	}
}
