package pilotfish

import (
	"Janus/baselines/common"
	"testing"
)

func TestPilotfishReadEntriesShareQueueLevel(t *testing.T) {
	tx1 := testPilotfishTx(1, []string{"a"}, nil)
	tx2 := testPilotfishTx(2, []string{"a"}, nil)

	plan := buildPilotfishPlan([]*pilotfishTransaction{tx1, tx2}, 4)

	if plan.indegree[tx1] != 0 || plan.indegree[tx2] != 0 {
		t.Fatalf("read-only transactions on the same key should not depend on each other: indegree tx1=%d tx2=%d", plan.indegree[tx1], plan.indegree[tx2])
	}
	if dependencyExists(plan, tx1, tx2) || dependencyExists(plan, tx2, tx1) {
		t.Fatalf("read-only transactions on the same key should not have dependency edges")
	}
	if got := len(plan.queues["a"]); got != 1 {
		t.Fatalf("expected one shared read queue entry, got %d", got)
	}
}

func TestPilotfishWriteBeforeReadDependency(t *testing.T) {
	writer := testPilotfishTx(1, nil, []string{"a"})
	reader := testPilotfishTx(2, []string{"a"}, nil)

	plan := buildPilotfishPlan([]*pilotfishTransaction{writer, reader}, 4)

	if !dependencyExists(plan, writer, reader) {
		t.Fatalf("reader should depend on the preceding writer")
	}
	if plan.indegree[reader] != 1 {
		t.Fatalf("expected reader indegree 1, got %d", plan.indegree[reader])
	}
}

func TestPilotfishReadBeforeWriteDependency(t *testing.T) {
	reader := testPilotfishTx(1, []string{"a"}, nil)
	writer := testPilotfishTx(2, nil, []string{"a"})

	plan := buildPilotfishPlan([]*pilotfishTransaction{reader, writer}, 4)

	if !dependencyExists(plan, reader, writer) {
		t.Fatalf("writer should depend on the preceding reader")
	}
	if plan.indegree[writer] != 1 {
		t.Fatalf("expected writer indegree 1, got %d", plan.indegree[writer])
	}
}

func TestPilotfishMultiObjectTransactionWaitsForAllQueues(t *testing.T) {
	writeA := testPilotfishTx(1, nil, []string{"a"})
	writeB := testPilotfishTx(2, nil, []string{"b"})
	readBoth := testPilotfishTx(3, []string{"a", "b"}, nil)

	plan := buildPilotfishPlan([]*pilotfishTransaction{writeA, writeB, readBoth}, 4)

	if !dependencyExists(plan, writeA, readBoth) {
		t.Fatalf("multi-object reader should depend on writer for key a")
	}
	if !dependencyExists(plan, writeB, readBoth) {
		t.Fatalf("multi-object reader should depend on writer for key b")
	}
	if plan.indegree[readBoth] != 2 {
		t.Fatalf("expected multi-object reader indegree 2, got %d", plan.indegree[readBoth])
	}
}

func TestPilotfishExecutionWorkerSelectionPrefersWrites(t *testing.T) {
	tx := testPilotfishTx(1, []string{"a"}, []string{"b"})
	plan := buildPilotfishPlan([]*pilotfishTransaction{tx}, 8)

	want := ownerForKey("b", 8)
	if tx.selectedWorker != want {
		t.Fatalf("expected selected worker %d, got %d", want, tx.selectedWorker)
	}
	if len(plan.readyWorkers()) != 1 {
		t.Fatalf("expected one ready worker")
	}
}

func testPilotfishTx(id uint32, reads, writes []string) *pilotfishTransaction {
	vertex := &common.TransactionVertex{
		ReadKeys:  make(map[string]string),
		WriteKeys: make(map[string]string),
	}
	for _, key := range reads {
		vertex.ReadKeys[key] = "value"
	}
	for _, key := range writes {
		vertex.WriteKeys[key] = "value"
	}
	return newPilotfishTransaction(common.NewBasicTransaction(id, 0, int(id), vertex, nil))
}

func dependencyExists(plan *pilotfishExecutionPlan, from, to *pilotfishTransaction) bool {
	for _, dependent := range plan.dependents[from] {
		if dependent == to {
			return true
		}
	}
	return false
}

func (p *pilotfishExecutionPlan) readyWorkers() []int {
	workers := make(map[int]struct{})
	for tx, indegree := range p.indegree {
		if indegree == 0 {
			workers[tx.selectedWorker] = struct{}{}
		}
	}
	result := make([]int, 0, len(workers))
	for worker := range workers {
		result = append(result, worker)
	}
	return result
}
