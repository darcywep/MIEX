package harmony

import (
	"Janus/baselines/common"
	"Janus/ethereum/core/types"
	"strings"
	"sync/atomic"
	"time"
)

// HarmonyTransaction harmony transaction with local read and write set.
type HarmonyTransaction struct {
	Tx           *common.BasicTransaction // 嵌入 Transaction，假设 Transaction 类型已存在
	EthTx        *types.Transaction
	ID           uint32
	BatchID      uint32
	FlagConflict bool
	Committed    atomic.Bool
	StartTime    time.Time
	LocalGet     map[string]string
	LocalPut     map[string]string
	MinOut       uint32
	MaxIn        uint32
	OutBatchID   uint32
	InBatchID    uint32
}

// NewHarmonyTransaction 构造函数
func NewHarmonyTransaction(inner *common.BasicTransaction, id uint32, batchID uint32) *HarmonyTransaction {
	return &HarmonyTransaction{
		Tx:         inner,
		ID:         id,
		BatchID:    batchID,
		MinOut:     id + 1,
		MaxIn:      0,
		OutBatchID: batchID,
		InBatchID:  batchID,
		StartTime:  time.Now(), // 使用当前时间作为开始时间
		LocalGet:   make(map[string]string),
		LocalPut:   make(map[string]string),
	}
}

func (executor *HarmonyExecutor) GetStorage_From_Table(tx *HarmonyTransaction, readSet map[string]bool) {
	var keys strings.Builder
	for key := range readSet {
		keys.WriteString(key + " ")
		var value string
		executor.table.table.Get(key, func(entry HarmonyEntry) {
			value = (entry).Value
		})

		tx.LocalGet[key] = value
	}
	//fmt.Printf("tx %d fallbacking, read: %s", tx.ID, keys.String())
}

func (executor *HarmonyExecutor) SetStorage_Into_Table(tx *HarmonyTransaction, writeSet map[string]bool, value string) {

	var keys strings.Builder
	for key := range writeSet {
		keys.WriteString(key + " ")
		executor.table.table.Put(key, func(entry *HarmonyEntry) {
			(*entry).Value = value
		})
	}
	//fmt.Printf("tx %d fallbacking, write: %s", tx.ID, keys.String())
}
