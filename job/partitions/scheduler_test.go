package partitions

import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/node"
	"testing"
)

func TestScheduler_AffinityRule(t *testing.T) {
	Convey("Given a partition.Scheduler", t, func() {
		Convey("When executors are sufficient", func() {
			nn := []*node.Node{
				{Host: "localhost:1001", Executors: 3, Tag: map[string]string{"CustomTag": "hello"}},
				{Host: "localhost:1002", Executors: 3, Tag: map[string]string{"CustomTag": "world"}},
				{Host: "localhost:1003", Executors: 3, Tag: map[string]string{"CustomTag": "foo"}},
				{Host: "localhost:1004", Executors: 3, Tag: map[string]string{"CustomTag": "bar"}},
			}
			s, err := NewSchedulerWithNodes(nil, nil, nn)
			So(err, ShouldBeNil)

			Convey("When an affinity rule is given with an LogicalPlanner", func() {
				l := logicalPlannerStub{LogicalPlans{
					{Key: "familiarWithWorld", NodeAffinityRules: map[string]string{"Host": "localhost:1002"}},
					{Key: "familiarWithFoo", NodeAffinityRules: map[string]string{"CustomTag": "foo"}},
					{Key: "familiarWithBar", NodeAffinityRules: map[string]string{"CustomTag": "bar"}},
					{Key: "familiarWithFreest"},
					{Key: "familiarWithWorld2", NodeAffinityRules: map[string]string{"Host": "localhost:1002"}},
					{Key: "familiarWithFoo2", NodeAffinityRules: map[string]string{"CustomTag": "foo"}},
					{Key: "familiarWithBar2", NodeAffinityRules: map[string]string{"CustomTag": "bar"}},
					{Key: "familiarWithFreest2"},
				}}
				_, pp := s.Plan(WithLogicalPlanner(l))
				So(pp, ShouldHaveLength, 8)

				keyToHostMap := pp.ToMap()
				So(keyToHostMap["familiarWithWorld"], ShouldEqual, "localhost:1002")
				So(keyToHostMap["familiarWithFoo"], ShouldEqual, "localhost:1003")
				So(keyToHostMap["familiarWithBar"], ShouldEqual, "localhost:1004")
				So(keyToHostMap["familiarWithFreest"], ShouldEqual, "localhost:1001")
				So(keyToHostMap["familiarWithWorld2"], ShouldEqual, "localhost:1002")
				So(keyToHostMap["familiarWithFoo2"], ShouldEqual, "localhost:1003")
				So(keyToHostMap["familiarWithBar2"], ShouldEqual, "localhost:1004")
				So(keyToHostMap["familiarWithFreest2"], ShouldEqual, "localhost:1001")
			})
		})

		Convey("When executors are scarce", func() {
			nn := []*node.Node{
				{Host: "localhost:1001", Executors: 1, Tag: map[string]string{"CustomTag": "hello"}},
				{Host: "localhost:1002", Executors: 1, Tag: map[string]string{"CustomTag": "world"}},
				{Host: "localhost:1003", Executors: 1, Tag: map[string]string{"CustomTag": "foo"}},
				{Host: "localhost:1004", Executors: 1, Tag: map[string]string{"CustomTag": "bar"}},
			}
			s, err := NewSchedulerWithNodes(nil, nil, nn)
			So(err, ShouldBeNil)

			Convey("When an affinity rule is given with an LogicalPlanner", func() {
				l := logicalPlannerStub{LogicalPlans{
					{Key: "familiarWithWorld", NodeAffinityRules: map[string]string{"Host": "localhost:1002"}},
					{Key: "familiarWithFoo", NodeAffinityRules: map[string]string{"CustomTag": "foo"}},
					{Key: "familiarWithBar", NodeAffinityRules: map[string]string{"CustomTag": "bar"}},
					{Key: "familiarWithFreest"},
					{Key: "familiarWithWorld2", NodeAffinityRules: map[string]string{"Host": "localhost:1002"}},
					{Key: "familiarWithFoo2", NodeAffinityRules: map[string]string{"CustomTag": "foo"}},
					{Key: "familiarWithBar2", NodeAffinityRules: map[string]string{"CustomTag": "bar"}},
					{Key: "familiarWithFreest2"},
				}}
				_, pp := s.Plan(WithLogicalPlanner(l))
				So(pp, ShouldHaveLength, 8)

				keyToHostMap := pp.ToMap()
				So(keyToHostMap["familiarWithWorld"], ShouldEqual, "localhost:1002")
				So(keyToHostMap["familiarWithFoo"], ShouldEqual, "localhost:1003")
				So(keyToHostMap["familiarWithBar"], ShouldEqual, "localhost:1004")
				So(keyToHostMap["familiarWithFreest"], ShouldEqual, "localhost:1001")
				So(keyToHostMap["familiarWithWorld2"], ShouldEqual, "localhost:1002")
				So(keyToHostMap["familiarWithFoo2"], ShouldEqual, "localhost:1003")
				So(keyToHostMap["familiarWithBar2"], ShouldEqual, "localhost:1004")
				So(keyToHostMap["familiarWithFreest2"], ShouldEqual, "localhost:1001")
			})
		})
	})
}

type logicalPlannerStub struct {
	Data LogicalPlans
}

func (l logicalPlannerStub) Plan() LogicalPlans {
	return l.Data
}
