package lrmrmetric

import (
	"sort"
	"strings"

	"github.com/ab180/lrmr/cluster/node"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// JobLabels are vector definitions for worker-level metrics.
var WorkerLabels = []string{"host", "tag"}

var RunningTasksGauge = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "lrmr_running_tasks",
		Help: "The current number of running tasks per node",
	},
	WorkerLabels,
)

// WorkerLabelValuesFrom extracts label values for worker-level metrics from a node information.
func WorkerLabelValuesFrom(n *node.Node) prometheus.Labels {
	// sort tags alphabetically to create a consistent label
	keys := make([]string, 0, len(n.Tag))
	for k := range n.Tag {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	entries := make([]string, len(n.Tag))
	for i, k := range keys {
		entries[i] = k + "=" + n.Tag[k]
	}
	tagLabel := strings.Join(entries, ",")
	return prometheus.Labels{
		"host": n.Host,
		"tag":  tagLabel,
	}
}
