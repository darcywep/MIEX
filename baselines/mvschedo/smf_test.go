package mvschedo

import (
	"Janus/baselines/common"
	"testing"
)

func TestIdentifyHotKeysRequiresCrossTransactionWriteConflict(t *testing.T) {
	tx1 := testMVSchedOTx(1, map[string]string{"shared": "value", "read-only": "value"}, map[string]string{"own": "value"})
	tx2 := testMVSchedOTx(2, map[string]string{"shared": "value", "read-only": "value"}, nil)
	tx3 := testMVSchedOTx(3, nil, map[string]string{"shared": "value"})

	hotKeys := identifyHotKeys([]*MVSchedOTransaction{tx1, tx2, tx3})
	if _, ok := hotKeys["shared"]; !ok {
		t.Fatalf("expected shared to be hot")
	}
	if _, ok := hotKeys["read-only"]; ok {
		t.Fatalf("did not expect read-only key to be hot")
	}
	if _, ok := hotKeys["own"]; ok {
		t.Fatalf("did not expect single-transaction write key to be hot")
	}
}

func TestSMFScheduleUsesOnlyHotKeysForMakespan(t *testing.T) {
	first := testMVSchedOTx(1, nil, map[string]string{"cold-a": "value"})
	second := testMVSchedOTx(2, nil, map[string]string{"cold-b": "value"})

	scheduler := NewSMFScheduler(2, 1)
	scheduled := scheduler.Schedule([]*MVSchedOTransaction{first, second}, map[string]struct{}{})

	if scheduled[0] != first || scheduled[1] != second {
		t.Fatalf("expected arrival order when there are no hot keys")
	}
	if first.Timestamp != 1 || second.Timestamp != 2 {
		t.Fatalf("expected timestamps to follow arrival order, got %d and %d", first.Timestamp, second.Timestamp)
	}
}

func TestScheduleQueuesUseGlobalHotKeys(t *testing.T) {
	tx := testMVSchedOTx(1, map[string]string{"global-hot": "value", "cold": "value"}, map[string]string{"global-hot": "value"})

	queues := NewScheduleQueues([]*MVSchedOTransaction{tx}, map[string]struct{}{"global-hot": {}})

	if _, ok := queues.queues["global-hot"]; !ok {
		t.Fatalf("expected queue for global hot key")
	}
	if _, ok := queues.queues["cold"]; ok {
		t.Fatalf("did not expect queue for cold key")
	}
}

func testMVSchedOTx(id uint32, reads, writes map[string]string) *MVSchedOTransaction {
	tx := NewMVSchedOTransaction(&common.BasicTransaction{Txid: id})
	copyStringMap(tx.LocalGet, reads)
	copyStringMap(tx.LocalPut, writes)
	tx.RebuildOperations()
	return tx
}
