package test

import (
	"testing"

	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/test/integration"
)

func TestNodeSelection(t *testing.T) {
	Convey("Given running nodes", t, func() {
		Convey("Running job with selecting particular nodes", integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
			Convey("It should be only ran on selected nodes", func() {
				j, err := NodeSelection(cluster.Session).Run()
				So(err, ShouldBeNil)

				So(j.Wait(), ShouldBeNil)

				m, err := j.Metrics()
				So(err, ShouldBeNil)

				// NumPartitions = (number of nodes) * 2 (=default concurrency in StartLocalCluster)
				So(m["NumPartitions"], ShouldEqual, 2)
			})
		}, lrmr.WithNodeSelector(map[string]string{"No": "1"})))

		Convey("Running job with selector which doesn't match any nodes", integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
			Convey("ErrNoAvailableWorkers should be raised", func() {
				_, err := NodeSelection(cluster.Session).Run()
				So(errors.Cause(err), ShouldEqual, master.ErrNoAvailableWorkers)
			})
		}, lrmr.WithNodeSelector(map[string]string{"going": "nowhere"})))
	})
}
