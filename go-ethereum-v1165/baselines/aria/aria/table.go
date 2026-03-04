package aria

import (
	optmeCommon "Janus/baselines/common"
)

// AriaEntry 表示 reservation 表中某个 key 的信息
type AriaEntry struct {
	Value       string
	BatchIDGet  uint64
	BatchIDPut  uint64
	ReservedGet *AriaTransaction
	ReservedPut *AriaTransaction
}

// AriaTable 组合 common.Table，用作 reservation / data table
type AriaTable struct {
	*optmeCommon.Table[AriaEntry]
}

// NewAriaTable
func NewAriaTable(partitions int) *AriaTable {
	return &AriaTable{
		Table: optmeCommon.NewTable[AriaEntry](partitions),
	}
}

// ReserveGet: 记录读预约（可选）
func (t *AriaTable) ReserveGet(tx *AriaTransaction, key string) {
	t.Table.PutWithDefault(key, AriaEntry{}, func(e *AriaEntry) {
		entry := e
		if entry.BatchIDGet != tx.BatchID || entry.ReservedGet == nil || entry.ReservedGet.ID > tx.ID {
			entry.ReservedGet = tx
			entry.BatchIDGet = tx.BatchID
		}
	})
}

// ReservePut: 为写集合做写预约，实现 min(TID) 语义（同一批次内较小 ID 覆盖）
func (t *AriaTable) ReservePut(tx *AriaTransaction, key string) {
	t.Table.PutWithDefault(key, AriaEntry{}, func(e *AriaEntry) {
		entry := *e
		if entry.BatchIDPut != tx.BatchID || entry.ReservedPut == nil {
			// 不同批次或尚无预约 -> 直接设置
			entry.ReservedPut = tx
			entry.BatchIDPut = tx.BatchID
		} else {
			// 同一批次已有预约 -> 保持 min(TID)
			if entry.ReservedPut.ID > tx.ID {
				entry.ReservedPut = tx
				entry.BatchIDPut = tx.BatchID
			}
			// 若已有预约 ID < tx.ID，则保持已有预约（tx 的预约名义上失败）
		}
		*e = entry
	})
}

// CompareReservedGet: 检查当前写是否与已有读预约冲突（用于 WAR 检测）
// 返回 true 表示允许（无冲突）
func (t *AriaTable) CompareReservedGet(tx *AriaTransaction, key string) bool {
	ok := true
	t.Table.Get(key, func(e AriaEntry) {
		if e.BatchIDGet == tx.BatchID && e.ReservedGet != nil && e.ReservedGet.ID < tx.ID {
			ok = false
		}
	})
	return ok
}

// CompareReservedPut: 检查 RAW / WAW 冲突：若同一批次存在 ReservedPut 且 ReservedPut.ID < tx.ID 则冲突
// 返回 true 表示允许（无冲突）
func (t *AriaTable) CompareReservedPut(tx *AriaTransaction, key string) bool {
	ok := true
	t.Table.Get(key, func(e AriaEntry) {
		if e.BatchIDPut == tx.BatchID && e.ReservedPut != nil && e.ReservedPut.ID < tx.ID {
			ok = false
		}
	})
	return ok
}
