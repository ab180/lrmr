package test

import (
	"testing"

	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testdata"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/goleak"
)

func TestComplicatedQuery(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Given running nodes", t, integration.WithLocalCluster(4, func(cluster *integration.LocalCluster) {
		Convey("When doing ComplicatedQuery", func() {
			ds := ComplicatedQuery(cluster.Session)
			j, err := ds.Run()
			So(err, ShouldBeNil)

			Convey("It should be run without error", func() {
				So(j.Wait(), ShouldBeNil)

				Convey("It should emit all metrics", func() {
					m, err := j.Metrics()
					So(err, ShouldBeNil)

					t.Logf("Metrics collected:\n%s", m.String())
					So(m["Files"], ShouldEqual, testdata.TotalFiles)
					So(m["Events"], ShouldEqual, testdata.TotalRows)
				})
			})
		})
	}))
}
