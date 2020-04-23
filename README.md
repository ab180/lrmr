lrmr
========

Less-Resilient MapReduce for Go.

 * Fast job scheduling
 * Easily scalable onto distributed clusters
 * Easily embeddable to existing applications
 * Uses **etcd** for cluster management / coordination

```go
package main

import (
	"context"
    "fmt"
	"github.com/therne/lrmr"
	. "github.com/therne/lrmr/playground"
)

func main() {
    m, err := lrmr.RunMaster()
    if err != nil {
        panic(err)
    }
    m.Start()
    defer m.Stop()

    dataset := lrmr.TextFile("./test-jsons/", m).
        FlatMap(DecodeJSON()).
        GroupByKey().
        Reduce(Count())

    job, err := dataset.Run(context.Background(), "GroupByApp")
    if err != nil {
        panic(err)
    }
    _ = job.Wait()
    fmt.Println("Done!", job.Metrics())
}
```

### Building and Developing lrmr

#### Requirements

 * Go 1.14 or above


## LICENSE: MIT
