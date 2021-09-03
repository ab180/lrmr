package test

import (
	"testing"

	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
)

func TestBroadcast(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When running Map with broadcasts", func() {
			ds := BroadcastTester()

			Convey("It should run without preserving broadcast values from master", func() {
				rows, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
				So(err, ShouldBeNil)
				So(rows.Outputs, ShouldHaveLength, 1)
				So(testutils.StringValue(rows.Outputs[0]), ShouldEqual, "throughStruct=foo, throughContext=bar, typeMatched=true")
			})
		})
	}))
}

/*
Fail Log:
2021-07-05 17:29:08.461 T │ lrmr(10.113ms): cold-snow (Jc2b1e951) started
2021-07-05 17:29:08.461 V │ lrmr: Pushing data from node master to Jc2b1e951/BroadcastStage0/3 finished (input channel closed)
2021-07-05 17:29:08.461 V │ lrmr.task.Jc2b1e951/BroadcastStage0/3: input closed
2021-07-05 17:29:08.462 V │ lrmr.task.Jc2b1e951/BroadcastStage0/3: executor succeeded
2021-07-05 17:29:08.462 V │ lrmr.jobReporter: Task Jc2b1e951/BroadcastStage0/3 succeeded after 3.254729ms
2021-07-05 17:29:08.462 V │ lrmr.task.Jc2b1e951/BroadcastStage0/3: TaskExecutor.close()
2021-07-05 17:29:08.462 V │ lrmr.jobReporter: Reporting succeeded stage Jc2b1e951/BroadcastStage0 (by Jc2b1e951/BroadcastStage0/3)
2021-07-05 17:29:08.462 V │ lrmr: Stage Jc2b1e951/BroadcastStage0 succeeded.
2021-07-05 17:29:08.462 V │ lrmr: Pushing data from node 127.0.0.1:60663 to Jc2b1e951/_collect/_collect finished (input channel closed)
2021-07-05 17:29:08.462 V │ lrmr.task.Jc2b1e951/_collect/_collect: executor succeeded
2021-07-05 17:29:08.462 V │ lrmr.task.Jc2b1e951/_collect/_collect: input closed
2021-07-05 17:29:08.462 V │ lrmr.jobReporter: Task Jc2b1e951/_collect/_collect succeeded after 5.197499ms
2021-07-05 17:29:08.462 V │ lrmr.jobReporter: Reporting succeeded stage Jc2b1e951/_collect (by Jc2b1e951/_collect/_collect)
2021-07-05 17:29:08.462 V │ lrmr: Stage Jc2b1e951/_collect succeeded.
2021-07-05 17:29:08.462 V │ lrmr.jobReporter: Reporting succeeded job Jc2b1e951 (by Jc2b1e951/_collect/_collect)
2021-07-05 17:29:08.462 V │ lrmr.task.Jc2b1e951/_collect/_collect: TaskExecutor.close()
2021-07-05 17:29:08.462 V │ lrmr: Job Jc2b1e951 context cancelling
2021-07-05 17:29:08.462 I │ lrmr: Job Jc2b1e951 succeeded. Total elapsed 10.572814ms
2021-07-05 17:29:08.462 I │ lrmr: Job cold-snow (Jc2b1e951) succeed. Collected 0 rows.
*/
