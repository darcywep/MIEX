package harmony

import (
	"Janus/baselines/common"
)

// HarmonyEntry harmony table entry for first round execution
type HarmonyEntry struct {
	Value          string
	ReservedGetTxs []*HarmonyTransaction
	ReservedPutTxs []*HarmonyTransaction
}

//// NewHarmonyEntry 构造函数
//func NewHarmonyEntry() *HarmonyEntry {
//	return &HarmonyEntry{
//		Value:          "",
//		ReservedGetTxs: make([]*HarmonyTransaction, 0),
//		ReservedPutTxs: make([]*HarmonyTransaction, 0),
//	}
//}

// HarmonyTable harmony table for first round execution
// HarmonyTable harmony table for first round execution
type HarmonyTable struct {
	table *common.Table[HarmonyEntry]
}

func NewHarmonyTable(partitions int) *HarmonyTable {
	return &HarmonyTable{
		table: common.NewTable[HarmonyEntry](partitions),
	}
}

// OnSeeingRWDependency handle a r-w dependency
// Ti the transaction write key
// Tj the transaction read key
func (ht *HarmonyTable) OnSeeingRWDependency(Ti *HarmonyTransaction, Tj *HarmonyTransaction) {
	//fmt.Printf("handle r-w dependency: %d:%d -> %d:%d", Tj.BatchID, Tj.ID, Ti.BatchID, Ti.ID)
	if Ti.ID < Tj.MinOut {
		Tj.MinOut = Ti.ID
		Tj.OutBatchID = Ti.BatchID
	}
	if Tj.ID > Ti.MaxIn {
		Ti.MaxIn = Tj.ID
		Ti.InBatchID = Tj.BatchID
	}
}

// HarmonyLockEntry harmony table entry for fallback pessimistic execution
type HarmonyLockEntry struct {
	DepsGet []*HarmonyTransaction
	DepsPut []*HarmonyTransaction
}

// NewHarmonyLockEntry 构造函数
func NewHarmonyLockEntry() *HarmonyLockEntry {
	return &HarmonyLockEntry{
		DepsGet: make([]*HarmonyTransaction, 0),
		DepsPut: make([]*HarmonyTransaction, 0),
	}
}

type HarmonyLockTable struct {
	table *common.Table[HarmonyLockEntry]
}

func NewHarmonyLockTable(partitions int) *HarmonyLockTable {
	return &HarmonyLockTable{
		common.NewTable[HarmonyLockEntry](partitions),
	}
}
