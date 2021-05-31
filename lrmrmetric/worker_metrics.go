package lrmrmetric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// JobLabels are vector definitions for worker-level metrics.
var WorkerLabels = []string{"host"}

var RunningTasksGauge = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "lrmr_running_tasks",
		Help: "The current number of running tasks per node",
	},
	WorkerLabels,
)
