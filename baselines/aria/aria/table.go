package aria

import (
	optmeCommon "Janus/baselines/common"
)

type AriaEntry struct {
	Value       string
	BatchIDGet  uint64
	BatchIDPut  uint64
	ReservedGet *AriaTransaction
	ReservedPut *AriaTransaction
}

// AriaTable 组合 common.Table，用于第一轮执行阶段
type AriaTable struct {
	*optmeCommon.Table[*AriaEntry]
}

// NewAriaTable 创建 Aria 表
func NewAriaTable(partitions int) *AriaTable {
	return &AriaTable{
		Table: optmeCommon.NewTable[*AriaEntry](partitions),
	}
}

// ReserveGet 记录当前事务对 key 的读保留
func (t *AriaTable) ReserveGet(tx *AriaTransaction, key string) {
	t.Table.PutWithDefault(key, &AriaEntry{}, func(e **AriaEntry) {
		entry := *e
		if entry.BatchIDGet != tx.BatchID || entry.ReservedGet == nil || entry.ReservedGet.ID > tx.ID {
			entry.ReservedGet = tx
			entry.BatchIDGet = tx.BatchID
		}
	})
}

// ReservePut 记录当前事务对 key 的写保留
func (t *AriaTable) ReservePut(tx *AriaTransaction, key string) {
	t.Table.PutWithDefault(key, &AriaEntry{}, func(e **AriaEntry) {
		entry := *e
		if entry.BatchIDPut != tx.BatchID || entry.ReservedPut == nil || entry.ReservedPut.ID > tx.ID {
			entry.ReservedPut = tx
			entry.BatchIDPut = tx.BatchID
		}
	})
}

// CompareReservedGet 检查读冲突（RAW）
func (t *AriaTable) CompareReservedGet(tx *AriaTransaction, key string) bool {
	ok := true
	t.Table.Get(key, func(e *AriaEntry) {
		if e.BatchIDGet == tx.BatchID && e.ReservedGet != nil && e.ReservedGet.ID != tx.ID {
			ok = false
		}
	})
	return ok
}

// CompareReservedPut 检查写冲突（WAW / WAR）
func (t *AriaTable) CompareReservedPut(tx *AriaTransaction, key string) bool {
	ok := true
	t.Table.Get(key, func(e *AriaEntry) {
		if e.BatchIDPut == tx.BatchID && e.ReservedPut != nil && e.ReservedPut.ID != tx.ID {
			ok = false
		}
	})
	return ok
}
