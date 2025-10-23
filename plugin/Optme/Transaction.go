package Optme

import (
	"Janus/plugin/Common"
	"fmt"
	"sync/atomic"
	"time"
)

type OptmeTransaction struct {
	tx         *Common.JanusTransaction
	blockid    uint32
	sequenceid uint32
	committed  atomic.Bool
	aborted    atomic.Bool

	StartTime time.Time

	// 本地读写缓存（模拟）
	LocalGet map[string]string
	LocalPut map[string]string
}

// 即将换成EVM逻辑
func (t *OptmeTransaction) Execute() {
	fmt.Printf("正在执行交易%d", t.tx.Txid)

	num := 0
	for i := 0; i < t.tx.Cost; i++ {
		num++
	}
}
