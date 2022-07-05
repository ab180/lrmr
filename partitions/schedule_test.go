package partitions

import (
	"testing"

	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/lrdd"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func TestNodeWithStatsSlice_New(t *testing.T) {
	nA := node.New("a")
	nB := node.New("b")
	nC := node.New("c")

	nodes := []*node.Node{
		nA,
		nB,
		nC,
	}

	expected := nodeWithStatsSlice{
		newNodeWithStats(nA),
		newNodeWithStats(nB),
		newNodeWithStats(nC),
	}
	actual := newNodeWithStatsSlice(nodes)

	require.Equal(t, expected, actual)
}

func TestScheduler_AffinityRule(t *testing.T) {
	Convey("Given a partition.Scheduler", t, func() {
		Convey("When executors are sufficient", func() {
			nn := []*node.Node{
				{Host: "localhost:1001", Executors: 3, Tag: map[string]string{"CustomTag": "hello"}},
				{Host: "localhost:1002", Executors: 3, Tag: map[string]string{"CustomTag": "world"}},
				{Host: "localhost:1003", Executors: 3, Tag: map[string]string{"CustomTag": "foo"}},
				{Host: "localhost:1004", Executors: 3, Tag: map[string]string{"CustomTag": "bar"}},
			}

			Convey("When partition counts in plans are all automatic", func() {
				pp, _ := Schedule(nn, []Plan{
					{DesiredCount: Auto},
					{DesiredCount: Auto},
					{DesiredCount: Auto},
					{DesiredCount: Auto},
				})

				Convey("First stage should have only one partition", func() {
					So(pp[0].Partitions, ShouldHaveLength, 1)
					checkPartitionerType(pp[0].Partitioner, NewShuffledPartitioner())
				})
				Convey("Rest of the stages should have number of partitions same as node count", func() {
					for i := 1; i < 3; i++ {
						So(pp[i].Partitions, ShouldHaveLength, 12)
						checkPartitionerType(pp[i].Partitioner, NewPreservePartitioner())
					}
				})
			})

			Convey("When an affinity rule is given with an Partitioner", func() {
				_, aa := Schedule(nn, []Plan{
					{Partitioner: partitionerStub{[]Partition{
						{ID: "familiarWithWorld", AssignmentAffinity: map[string]string{"Host": "localhost:1002"}},
						{ID: "familiarWithFoo", AssignmentAffinity: map[string]string{"CustomTag": "foo"}},
						{ID: "familiarWithBar", AssignmentAffinity: map[string]string{"CustomTag": "bar"}},
						{ID: "familiarWithFreest"},
						{ID: "familiarWithWorld2", AssignmentAffinity: map[string]string{"Host": "localhost:1002"}},
						{ID: "familiarWithFoo2", AssignmentAffinity: map[string]string{"CustomTag": "foo"}},
						{ID: "familiarWithBar2", AssignmentAffinity: map[string]string{"CustomTag": "bar"}},
						{ID: "familiarWithFreest2"},
					}}},
					{ /* ignored */ },
				}, WithoutShufflingNodes())
				So(aa, ShouldHaveLength, 2)
				So(aa[1], ShouldHaveLength, 8)

				keyToHostMap := aa[1].ToMap()
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

			Convey("When an affinity rule is given with an LogicalPlanner", func() {
				_, pp := Schedule(nn, []Plan{
					{Partitioner: partitionerStub{[]Partition{
						{ID: "familiarWithWorld", AssignmentAffinity: map[string]string{"Host": "localhost:1002"}},
						{ID: "familiarWithFoo", AssignmentAffinity: map[string]string{"CustomTag": "foo"}},
						{ID: "familiarWithBar", AssignmentAffinity: map[string]string{"CustomTag": "bar"}},
						{ID: "familiarWithFreest"},
						{ID: "familiarWithWorld2", AssignmentAffinity: map[string]string{"Host": "localhost:1002"}},
						{ID: "familiarWithFoo2", AssignmentAffinity: map[string]string{"CustomTag": "foo"}},
						{ID: "familiarWithBar2", AssignmentAffinity: map[string]string{"CustomTag": "bar"}},
						{ID: "familiarWithFreest2"},
					}}},
					{ /* ignored */ },
				}, WithoutShufflingNodes())
				So(pp, ShouldHaveLength, 2)
				So(pp[1], ShouldHaveLength, 8)

				keyToHostMap := pp[1].ToMap()
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

		Convey("When executors have same set of tag", func() {
			nn := []*node.Node{
				{Host: "localhost:1001", Executors: 1, Tag: map[string]string{"CustomTag": "hello"}},
				{Host: "localhost:1002", Executors: 1, Tag: map[string]string{"CustomTag": "hello"}},
				{Host: "localhost:1003", Executors: 1, Tag: map[string]string{"CustomTag": "hello"}},
				{Host: "localhost:1004", Executors: 1, Tag: map[string]string{"CustomTag": "hello"}},
			}

			Convey("When an affinity rule is given with an LogicalPlanner", func() {
				_, pp := Schedule(nn, []Plan{
					{Partitioner: partitionerStub{[]Partition{
						{ID: "p1", AssignmentAffinity: map[string]string{"CustomTag": "hello"}},
						{ID: "p2", AssignmentAffinity: map[string]string{"CustomTag": "hello"}},
						{ID: "p3", AssignmentAffinity: map[string]string{"CustomTag": "hello"}},
						{ID: "p4", AssignmentAffinity: map[string]string{"CustomTag": "hello"}},
					}}},
					{ /* ignored */ },
				}, WithoutShufflingNodes())

				Convey("It should be distributed evenly", func() {
					So(pp, ShouldHaveLength, 2)
					So(pp[1], ShouldHaveLength, 4)

					partitionsPerHost := pp[1].GroupIDsByHost()
					So(partitionsPerHost["localhost:1001"], ShouldHaveLength, 1)
					So(partitionsPerHost["localhost:1002"], ShouldHaveLength, 1)
					So(partitionsPerHost["localhost:1003"], ShouldHaveLength, 1)
					So(partitionsPerHost["localhost:1004"], ShouldHaveLength, 1)
				})
			})
		})
	})
}

type partitionerStub struct {
	Partitions []Partition
}

func (p partitionerStub) PlanNext(int) []Partition {
	return p.Partitions
}

func (p partitionerStub) DeterminePartition(c Context, r *lrdd.Row, numOutputs int) (id string, err error) {
	return
}

func checkPartitionerType(actual, expected Partitioner) {
	if sp, ok := actual.(SerializablePartitioner); ok {
		actual = sp.Partitioner
	}
	So(actual, ShouldHaveSameTypeAs, expected)
}
