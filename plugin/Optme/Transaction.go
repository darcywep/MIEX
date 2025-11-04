package Optme

import (
	"Janus/plugin/Common"
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
	return &OptmeTransaction{Tx: tx, Blockid: blockid, LocalGet: make(map[string]string), LocalPut: make(map[string]string)}
}

// 即将换成EVM逻辑
func (t *OptmeTransaction) Execute() {
	//fmt.Printf("正在执行交易:%d \n", t.Tx.Txid)
	num := 0
	for i := 0; i < int(t.Tx.Cost); i++ {
		num++
	}
}
