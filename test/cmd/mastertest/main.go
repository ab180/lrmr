package main

import (
	"context"

	"github.com/airbloc/logger"
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/test"
)

var log = logger.New("master")

func main() {
	m, err := lrmr.RunMaster()
	if err != nil {
		log.Fatal("failed to start master", err)
	}
	m.Start()
	defer m.Stop()

	sess := lrmr.NewSession(context.TODO(), m, lrmr.WithName("GroupByApp"))

	ds := sess.FromFile("/Users/vista/testdata/").
		WithWorkerCount(8).
		FlatMap(test.DecodeJSON()).
		GroupByKnownKeys([]string{"1737", "777", "1364", "6038"}).
		Reduce(test.Count())

	j, err := ds.Run()
	if err != nil {
		log.Fatal("failed to run session", err)
	}
	if err := j.Wait(); err != nil {
		log.Fatal(err.Error())
	}

	// print metrics
	metrics, err := j.Metrics()
	if err != nil {
		log.Warn("failed to collect metric: {}", err)
	}
	log.Info("{} metrics have been collected.", len(metrics))
	for k, v := range metrics {
		log.Info("    {} = {}", k, v)
	}

	if j.Status() == job.Succeeded {
		log.Info("Done!")
	} else {
		log.Fatal("Failed.")
	}
}
