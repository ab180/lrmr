package test

import (
	"testing"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

func TestNodeSelection(t *testing.T) {
	Convey("Given running nodes", t, func() {
		Convey("Running job with selecting particular nodes", integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
			Convey("It should be only ran on selected nodes", func() {
				j, err := NodeSelection().RunInBackground(cluster)
				So(err, ShouldBeNil)

				So(j.WaitWithContext(testutils.ContextWithTimeout()), ShouldBeNil)

				m, err := j.Metrics()
				So(err, ShouldBeNil)

				// NumPartitions = (number of nodes) * 2 (=default concurrency in StartLocalCluster)
				So(m["NumPartitions"], ShouldEqual, 2)
			})
		}, lrmr.WithNodeSelector(map[string]string{"No": "1"})))

		Convey("Running job with selector which doesn't match any nodes", integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
			Convey("ErrNoAvailableWorkers should be raised", func() {
				_, err := NodeSelection().RunInBackground(cluster)
				So(errors.Cause(err), ShouldEqual, lrmr.ErrNoAvailableExecutors)
			})
		}, lrmr.WithNodeSelector(map[string]string{"going": "nowhere"})))
	})
}
