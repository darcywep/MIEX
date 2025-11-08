package Harmony

import (
	"Janus/core/types"
	"Janus/plugin/Common"
	"sync/atomic"
	"time"
)

// HarmonyTransaction harmony transaction with local read and write set.
type HarmonyTransaction struct {
	Transaction  *Common.JanusTransaction // 嵌入 Transaction，假设 Transaction 类型已存在
	Eth_Tx       *types.EthTransaction
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
func NewHarmonyTransaction(inner *Common.JanusTransaction, ethTx *types.EthTransaction, id uint32, batchID uint32) *HarmonyTransaction {
	return &HarmonyTransaction{
		Transaction: inner,
		Eth_Tx:      ethTx,
		ID:          id,
		BatchID:     batchID,
		StartTime:   time.Now(), // 使用当前时间作为开始时间
		LocalGet:    make(map[string]string),
		LocalPut:    make(map[string]string),
	}
}
