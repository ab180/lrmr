lrmr
========

Online MapReduce framework for Go, which is capable for jobs in sub-second.

 * Sacrificing resilience for fast performance
 * Easily scalable onto distributed clusters
 * Easily embeddable to existing applications
 * Uses **etcd** for cluster management / coordination

```go
package main

import (
    "context"
    "fmt"
    "github.com/ab180/lrmr"
    . "github.com/ab180/lrmr/test"
)

func main() {
    m, err := lrmr.RunMaster()
    if err != nil {
        panic(err)
    }
    m.Start()
    defer m.Stop()

    result, err := lrmr.NewSession(context.TODO(), m).
        FromFile("./testdata/").
        FlatMap(DecodeCSV()).
        GroupByKey().
        Reduce(Count()).
        Collect()

    if err != nil {
        panic(err)
    }
    fmt.Println("Done!", result)
}
```

### Building and Developing lrmr

#### Requirements

 * Go 1.14 or above


## LICENSE: MIT
