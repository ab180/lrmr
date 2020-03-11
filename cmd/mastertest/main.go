package main

import (
	"context"
	"fmt"
	"github.com/therne/lrmr"
	. "github.com/therne/lrmr/playground"
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

	sess := lrmr.TextFile("/Users/vista/testdata/", m).
		FlatMap(DecodeJSON()).
		GroupByKey().
		Reduce(Count())

	job, err := sess.Run(context.TODO(), "GroupByApp")
	if err != nil {
		fmt.Println("run session:", err.Error())
		os.Exit(1)
	}
	if _, err := job.WaitForResult(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	fmt.Println("Done!")
}
