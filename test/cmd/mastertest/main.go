package main

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/test"
	"github.com/ab180/lrmr/test/testdata"
	"github.com/rs/zerolog/log"
)

func main() {
	cluster, err := lrmr.ConnectToCluster()
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("failed to start master")
	}
	defer cluster.Close()

	j, err := lrmr.FromLocalFile(testdata.Path()).
		WithWorkerCount(8).
		FlatMap(test.DecodeCSV()).
		GroupByKnownKeys([]string{"1737", "777", "1364", "6038"}).
		Reduce(test.Count()).
		RunInBackground(cluster)

	if err != nil {
		log.Fatal().
			Err(err).
			Msg("failed to run session")
	}
	if err := j.Wait(); err != nil {
		log.Fatal().
			Err(err).
			Msg("failed to wait job")
	}

	// print metrics
	metrics, err := j.Metrics()
	if err != nil {
		log.Warn().
			Err(err).
			Msg("failed to collect metric")
	}
	log.Info().
		Interface("metrics", metrics).
		Msg("metrics have been collected.")

	if j.Status() == job.Succeeded {
		log.Info().Msg("done!")
	} else {
		log.Fatal().Msg("failed.")
	}
}
