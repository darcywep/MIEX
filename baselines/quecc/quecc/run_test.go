package quecc

import (
	"Janus/baselines/common"
	"math/rand"
	"testing"
)

func TestExecutionPriorityInvariant(t *testing.T) {
	highTx := &queccTransaction{priorityGroup: 0}
	lowTx := &queccTransaction{priorityGroup: 1}
	highQueue := &executionQueue{priorityGroup: 0, rangeID: 0, tokens: []*queccTransaction{highTx}}
	lowQueue := &executionQueue{priorityGroup: 1, rangeID: 0, tokens: []*queccTransaction{lowTx}}
	highTx.queues = []*executionQueue{highQueue}
	lowTx.queues = []*executionQueue{lowQueue}

	plan := &executionPlan{
		queues: [][]*executionQueue{
			{highQueue},
			{lowQueue},
		},
		allTxs:     []*queccTransaction{highTx, lowTx},
		indegree:   map[*queccTransaction]int{highTx: 0, lowTx: 0},
		dependents: make(map[*queccTransaction][]*queccTransaction),
		remaining:  2,
	}
	plan.buildDependencies()

	if got := plan.indegree[highTx]; got != 0 {
		t.Fatalf("higher-priority transaction indegree = %d, want 0", got)
	}
	if got := plan.indegree[lowTx]; got != 1 {
		t.Fatalf("lower-priority transaction indegree = %d, want 1", got)
	}
}

func TestWholeTransactionExecutionAcrossQueues(t *testing.T) {
	stats := common.NewStatistics()
	engine := &QueCC{
		statistics: stats,
		numThreads: 2,
		rangeCount: 2,
	}

	tx1 := &queccTransaction{
		inner: &common.BasicTransaction{
			Txid:   1,
			Vertex: &common.TransactionVertex{ReadKeys: map[string]string{}, WriteKeys: map[string]string{}},
		},
		rangeIDs: []int{0, 1},
	}
	tx2 := &queccTransaction{
		inner: &common.BasicTransaction{
			Txid:   2,
			Vertex: &common.TransactionVertex{ReadKeys: map[string]string{}, WriteKeys: map[string]string{}},
		},
		rangeIDs: []int{0},
	}

	plan := engine.planBlock([]*queccTransaction{tx1, tx2})
	engine.executePlan(plan)
	engine.commitPlan(plan)

	if got := stats.CommitCount.Load(); got != 2 {
		t.Fatalf("committed transactions = %d, want 2", got)
	}
	if !tx1.completed || !tx2.completed {
		t.Fatal("all whole transactions should complete")
	}
}

func TestRandomOverlappingQueuesMakeProgress(t *testing.T) {
	rng := rand.New(rand.NewSource(20270610))
	for round := 0; round < 100; round++ {
		stats := common.NewStatistics()
		engine := &QueCC{
			statistics: stats,
			numThreads: 4,
			rangeCount: 4,
		}

		txs := make([]*queccTransaction, 64)
		for txID := range txs {
			rangeSet := make(map[int]struct{})
			targetRangeCount := 1 + rng.Intn(engine.rangeCount)
			for len(rangeSet) < targetRangeCount {
				rangeSet[rng.Intn(engine.rangeCount)] = struct{}{}
			}

			rangeIDs := make([]int, 0, len(rangeSet))
			for rangeID := range rangeSet {
				rangeIDs = append(rangeIDs, rangeID)
			}
			txs[txID] = &queccTransaction{
				inner: &common.BasicTransaction{
					Txid:   uint32(txID + 1),
					Vertex: &common.TransactionVertex{ReadKeys: map[string]string{}, WriteKeys: map[string]string{}},
				},
				rangeIDs: rangeIDs,
			}
		}

		plan := engine.planBlock(txs)
		engine.executePlan(plan)
		engine.commitPlan(plan)
		if got := stats.CommitCount.Load(); got != uint32(len(txs)) {
			t.Fatalf("round %d committed transactions = %d, want %d", round, got, len(txs))
		}
	}
}
