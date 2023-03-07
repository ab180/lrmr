lrmr
========

Online MapReduce framework for Go, which is capable for jobs in sub-second.

 * Sacrificing resilience for fast performance
 * Easily scalable onto distributed clusters
 * Easily embeddable to existing applications
 * Uses **etcd** for cluster management / coordination

### Example (Driver)

```go
package main

import (
	"context"
	"fmt"

	"github.com/ab180/lrmr"
	. "github.com/ab180/lrmr/test"
)

func main() {
	cluster, err := lrmr.ConnectToCluster()
	if err != nil {
		panic(err)
	}
	defer cluster.Close()

	result, err := lrmr.FromLocalFile("./test/testdata/unpacked/").
		FlatMap(DecodeCSV()).
		GroupByKey().
		Reduce(Count()).
		RunAndCollect(context.Background(), cluster)

	if err != nil {
		panic(err)
	}
	fmt.Println("Outputs:", result.Ouptuts)
	fmt.Println("Metrics:", result.Metrics.String())
}
```

### Example (Executor)

Executor is a worker in a distributed cluster which runs jobs submitted from the driver.

```go
package main

import (
	"context"
	"fmt"

	"github.com/ab180/lrmr"
	. "github.com/ab180/lrmr/test"
)

func main() {
	c, err := lrmr.ConnectToCluster()
	if err != nil {
		log.Fatalf("failed to join cluster: %v", err)
	}
	exec, err := lrmr.NewExecutor(c, opt)
	if err != nil {
		log.Fatalf("failed to initiate executor: %v", err)
	}
	defer exec.Close()

	if err := exec.Start(); err != nil {
		log.Fatalf("failed to start executor: %v", err)
	}
}
```

### Building and Developing lrmr

#### Requirements

 * Go 1.20 or above


## LICENSE: MIT
