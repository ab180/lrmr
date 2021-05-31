package lrmrmetric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var RunningJobsGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "lrmr_running_jobs",
	Help: "The current number of running jobs",
})

var JobDurationSummary = promauto.NewSummary(prometheus.SummaryOpts{
	Name: "lrmr_job_duration_sec",
	Help: "MapReduce job execution duration in seconds",
})
