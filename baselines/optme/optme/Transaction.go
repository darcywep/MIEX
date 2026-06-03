package optme

import (
	"Janus/baselines/common"
	lvm "Janus/core/evm"
	"Janus/tools"

	"github.com/holiman/uint256"

	"sync/atomic"
	"time"
)

type OptmeTransaction struct {
	Tx              *common.BasicTransaction
	originalTxID    int
	originalBlockID int
	Blockid         uint32
	Sequenceid      uint32
	Committed       atomic.Bool
	Aborted         atomic.Bool

	StartTime time.Time

	// 本地读写缓存（模拟）
	LocalGet map[string]string
	LocalPut map[string]string
}

func NewOptmeTransaction(tx *common.BasicTransaction, blockid uint32, originalTxID, originalBlockID int) *OptmeTransaction {
	return &OptmeTransaction{
		Tx:              tx,
		Blockid:         blockid,
		originalTxID:    originalTxID,
		originalBlockID: originalBlockID,
		LocalGet:        make(map[string]string),
		LocalPut:        make(map[string]string),
	}
}

// 即将换成EVM逻辑
func (t *OptmeTransaction) Execute(levm *lvm.LEVM) {
	//tools.CatStorageState = true
	if !tools.ExecuteSimulatedTransaction(t.Tx.EthTx) {
		_, err := levm.CallContract(*t.Tx.EthTx.From(), *t.Tx.EthTx.To(), t.Tx.EthTx.Data(), new(uint256.Int).SetUint64(0))
		tools.PanicError("OptmeTransaction Execute", err)
	}
	// 真实负载直接使用 LatencyDB 读写集；合成负载仍回退到 SmallBank 规则。
	tools.FillStringReadWriteSet(t.Tx.EthTx, t.Tx.Vertex.ReadKeys, t.Tx.Vertex.WriteKeys)
}
