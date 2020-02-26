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

    job := lrmr.TextFile("./test-jsons/", m).
        Then(DecodeNDJSON()).
        GroupByKey("appID").
        Then(CountByApp())

    if err := job.Run(context.Background(), "GroupByApp"); err != nil {
        panic(err)
    }
    println("Done!")
}
```

### Building and Developing lrmr

#### Requirements

 * [flatbuffers](https://github.com/google/flatbuffers)
    * on macOS: `brew install flatbuffers`
    * on Windows: [Download EXE releases](https://github.com/google/flatbuffers/releases)
    * on Linux: [Manually building with CMake](https://google.github.io/flatbuffers/flatbuffers_guide_building.html)



## LICENSE: MIT
