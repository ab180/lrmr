package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
	lrmrmetric "github.com/ab180/lrmr/metric"
	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testdata"
	"github.com/ab180/lrmr/test/testutils"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/goleak"
)

func TestConcurrentRun(t *testing.T) {
	defer goleak.VerifyNone(t)
	const (
		N = 3
	)
	Convey("Running jobs concurrently", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("With RunAndCollect()", func() {
			Convey("It should return correct results with no error", func() {
				resultsChan := make(chan map[string][]*lrdd.Row, N)
				errChan := make(chan error, 1)
				for i := 0; i < N; i++ {
					i := i
					go func() {
						res, err := BasicGroupByKey(lrmr.WithName(fmt.Sprintf("collect-%d", i))).
							RunAndCollect(context.TODO(), cluster)

						if err != nil {
							errChan <- errors.Wrapf(err, "error on %dth run", i)
						}

						resultsChan <- testutils.GroupRowsByKey(res.Outputs())
						err = res.Err()
						if err != nil {
							errChan <- errors.Wrapf(err, "error on %dth run", i)
						}
					}()
				}
				for success := 0; success < N; success++ {
					select {
					case res := <-resultsChan:
						// validate results
						So(testutils.IntValue(res["8263"][0]), ShouldEqual, 197206)

					case err := <-errChan:
						So(err, ShouldBeNil)
					}
				}
			})
		})

		Convey("With RunInBackground()", func() {
			Convey("It should return correct results with no error", func() {
				resultsChan := make(chan lrmrmetric.Metrics, N)
				errChan := make(chan error, 1)
				for i := 0; i < N; i++ {
					i := i
					go func() {
						job, err := BasicGroupByKey(lrmr.WithName(fmt.Sprintf("background-%d", i))).
							RunInBackground(cluster)

						if err != nil {
							errChan <- errors.Wrapf(err, "starting %dth run", i)
						}
						if err := job.WaitWithContext(context.TODO()); err != nil {
							errChan <- errors.Wrapf(err, "waiting %dth run", i)
						}
						m, err := job.Metrics()
						if err != nil {
							errChan <- errors.Wrapf(err, "gathering metrics of %dth run", i)
						}
						resultsChan <- m
					}()
				}
				for success := 0; success < N; success++ {
					select {
					case m := <-resultsChan:
						// validate results
						So(m, ShouldResemble, lrmrmetric.Metrics{
							"Files":  testdata.TotalFiles,
							"Events": testdata.TotalRows,
						})

					case err := <-errChan:
						So(err, ShouldBeNil)
					}
				}
			})
		})
	}))
}
