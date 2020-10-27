package test

import (
	"testing"

	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/test/testutils"
)

func TestNodeSelection(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		Convey("Running job with selecting particular nodes", func() {
			cluster := testutils.StartLocalCluster(c, 2, lrmr.WithNodeSelector(map[string]string{"No": "1"}))
			defer cluster.Stop()

			Convey("It should be only ran on selected nodes", func() {
				j, err := NodeSelection(cluster.Session).Run()
				So(err, ShouldBeNil)

				So(j.Wait(), ShouldBeNil)

				m, err := j.Metrics()
				So(err, ShouldBeNil)

				// NumPartitions = (number of nodes) * 2 (=default concurrency in StartLocalCluster)
				So(m["countNumPartitions0/NumPartitions"], ShouldEqual, 2)
			})
		})

		Convey("Running job with selector which doesn't match any nodes", func() {
			cluster := testutils.StartLocalCluster(c, 2, lrmr.WithNodeSelector(map[string]string{"going": "nowhere"}))
			defer cluster.Stop()

			Convey("ErrNoAvailableWorkers should be raised", func() {
				_, err := NodeSelection(cluster.Session).Run()
				So(errors.Cause(err), ShouldEqual, master.ErrNoAvailableWorkers)
			})
		})
	})
}
