package Optme

import (
	"Janus/plugin/Common"
	"fmt"
	"sync/atomic"
	"time"
)

type OptmeTransaction struct {
	Tx         *Common.JanusTransaction
	Blockid    uint32
	Sequenceid uint32
	Committed  atomic.Bool
	Aborted    atomic.Bool

	StartTime time.Time

	// 本地读写缓存（模拟）
	LocalGet map[string]string
	LocalPut map[string]string
}

func NewOptmeTransaction(tx *Common.JanusTransaction, blockid uint32) *OptmeTransaction {
	return &OptmeTransaction{Tx: tx, Blockid: blockid}
}

// 即将换成EVM逻辑
func (t *OptmeTransaction) Execute() {
	fmt.Printf("正在执行交易%d", t.Tx.Txid)

	num := 0
	for i := 0; i < t.Tx.Cost; i++ {
		num++
	}
}
