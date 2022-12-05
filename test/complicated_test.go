package test

import (
	"testing"
	"time"

	lrmrmetric "github.com/ab180/lrmr/metric"
	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testdata"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/net/context"
)

func TestComplicatedQuery(t *testing.T) {
	defer goleak.VerifyNone(t)

	integration.WithLocalCluster(4, func(cluster *integration.LocalCluster) {
		ds := ComplicatedQuery()
		j, err := ds.RunInBackground(cluster)
		require.Nil(t, err)

		ctx, _ := context.WithTimeout(context.Background(), 20*time.Second)
		err = j.WaitWithContext(ctx)
		require.Nil(t, err)

		m, err := j.Metrics()
		require.Nil(t, err)

		t.Logf("Metrics collected:\n%s", m.String())

		require.Equal(t, testdata.TotalFiles, int(m["Files"]))
		require.Equal(t, testdata.TotalRows, int(m["Events"]))

		// check prometheus metric; number of running tasks should be 0
		metric := &dto.Metric{}
		for _, w := range cluster.Executors {
			err := lrmrmetric.RunningTasksGauge.
				With(lrmrmetric.WorkerLabelValuesFrom(w.Node.Info())).
				Write(metric)
			require.Nil(t, err)

			require.Equal(t, float64(0), metric.Gauge.GetValue())
		}
	})()
}
