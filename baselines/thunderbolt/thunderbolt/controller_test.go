package thunderbolt

import (
	"Janus/baselines/common"
	"testing"
)

func TestThunderboltReadOnlyTransactionsDoNotConflict(t *testing.T) {
	tx1 := testThunderboltTx(1, 1, []string{"a"}, nil)
	tx2 := testThunderboltTx(2, 2, []string{"a"}, nil)

	plan := buildThunderboltPlan([]*thunderboltTransaction{tx1, tx2})

	if plan.indegree[tx1] != 0 || plan.indegree[tx2] != 0 {
		t.Fatalf("read-only transactions should not depend on each other: tx1=%d tx2=%d", plan.indegree[tx1], plan.indegree[tx2])
	}
	if thunderboltDependencyExists(plan, tx1, tx2) || thunderboltDependencyExists(plan, tx2, tx1) {
		t.Fatalf("read-only transactions on the same key should not have dependency edges")
	}
}

func TestThunderboltWriteBeforeReadDependency(t *testing.T) {
	writer := testThunderboltTx(1, 1, nil, []string{"a"})
	reader := testThunderboltTx(2, 2, []string{"a"}, nil)

	plan := buildThunderboltPlan([]*thunderboltTransaction{writer, reader})

	if !thunderboltDependencyExists(plan, writer, reader) {
		t.Fatalf("reader should depend on the latest writer")
	}
	if plan.indegree[reader] != 1 {
		t.Fatalf("expected reader indegree 1, got %d", plan.indegree[reader])
	}
}

func TestThunderboltReadBeforeWriteDependency(t *testing.T) {
	reader := testThunderboltTx(1, 1, []string{"a"}, nil)
	writer := testThunderboltTx(2, 2, nil, []string{"a"})

	plan := buildThunderboltPlan([]*thunderboltTransaction{reader, writer})

	if !thunderboltDependencyExists(plan, reader, writer) {
		t.Fatalf("writer should wait for preceding readers on the same key")
	}
	if plan.indegree[writer] != 1 {
		t.Fatalf("expected writer indegree 1, got %d", plan.indegree[writer])
	}
}

func TestThunderboltRuntimeOrderReschedulesWrites(t *testing.T) {
	slowerArrival := testThunderboltTx(1, 2, nil, []string{"a"})
	fasterRuntime := testThunderboltTx(2, 1, nil, []string{"a"})

	plan := buildThunderboltPlan([]*thunderboltTransaction{slowerArrival, fasterRuntime})

	if !thunderboltDependencyExists(plan, fasterRuntime, slowerArrival) {
		t.Fatalf("runtime-completed writer should precede later-completed writer")
	}
	if plan.order[0] != fasterRuntime || plan.order[1] != slowerArrival {
		t.Fatalf("unexpected topological order")
	}
}

func TestThunderboltMultiKeyTransactionWaitsForAllDependencies(t *testing.T) {
	writeA := testThunderboltTx(1, 1, nil, []string{"a"})
	writeB := testThunderboltTx(2, 2, nil, []string{"b"})
	readBoth := testThunderboltTx(3, 3, []string{"a", "b"}, nil)

	plan := buildThunderboltPlan([]*thunderboltTransaction{writeA, writeB, readBoth})

	if !thunderboltDependencyExists(plan, writeA, readBoth) {
		t.Fatalf("reader should depend on writer for key a")
	}
	if !thunderboltDependencyExists(plan, writeB, readBoth) {
		t.Fatalf("reader should depend on writer for key b")
	}
	if plan.indegree[readBoth] != 2 {
		t.Fatalf("expected reader indegree 2, got %d", plan.indegree[readBoth])
	}
}

func testThunderboltTx(id uint32, preplayOrder int, reads, writes []string) *thunderboltTransaction {
	readKeys := make(map[string]string)
	for _, key := range reads {
		readKeys[key] = "value"
	}
	writeKeys := make(map[string]string)
	for _, key := range writes {
		writeKeys[key] = "value"
	}
	tx := newThunderboltTransaction(&common.BasicTransaction{
		Txid: id,
		Vertex: &common.TransactionVertex{
			ReadKeys:  readKeys,
			WriteKeys: writeKeys,
		},
	})
	tx.preplayOrder = preplayOrder
	return tx
}

func thunderboltDependencyExists(plan *thunderboltExecutionPlan, from, to *thunderboltTransaction) bool {
	for _, dependent := range plan.dependents[from] {
		if dependent == to {
			return true
		}
	}
	return false
}
