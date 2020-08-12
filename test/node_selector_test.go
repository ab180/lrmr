package test

import (
	"testing"

	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/test/testutils"
)

func TestNodeSelection(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		sess, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("Running job with selecting particular nodes", func() {
			ds := NodeSelection(sess, map[string]string{"No": "1"})
			j, err := ds.Run()
			So(err, ShouldBeNil)

			Convey("It should be only ran on selected nodes", func() {
				So(j.Wait(), ShouldBeNil)

				m, err := j.Metrics()
				So(err, ShouldBeNil)

				// NumPartitions = (number of nodes) * 2 (=default concurrency in StartLocalCluster)
				So(m["countNumPartitions0/NumPartitions"], ShouldEqual, 2)
			})
		})

		Convey("Running job with selector which doesn't match any nodes", func() {
			ds := NodeSelection(sess, map[string]string{"going": "nowhere"})

			Convey("ErrNoAvailableWorkers should be raised", func() {
				_, err := ds.Run()
				So(errors.Cause(err), ShouldEqual, master.ErrNoAvailableWorkers)
			})
		})
	})
}
