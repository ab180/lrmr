package main

import (
	"context"
	"fmt"
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/playground"
	"os"
)

func main() {
	m, err := lrmr.RunMaster()
	if err != nil {
		fmt.Println("error starting master:", err.Error())
		os.Exit(1)
	}
	m.Start()
	defer m.Stop()

	job := lrmr.Input(m, "/Users/vista/testdata/").
		AddStage("asJson", playground.NewLDJSONDecoder()).
		AddStage("counter", &playground.Counter{})

	if err := job.Run(context.TODO(), "CountTestData"); err != nil {
		fmt.Println("error running job:", err.Error())
		os.Exit(1)
	}
	fmt.Println("Done!")
}
