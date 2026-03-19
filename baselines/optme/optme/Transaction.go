package optme

import (
	"Janus/baselines/common"
	"Janus/config"
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
	_, err := levm.CallContract(*t.Tx.EthTx.From(), *t.Tx.EthTx.To(), t.Tx.EthTx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("OptmeTransaction Execute", err)

	if t.Tx.EthTx.TxType == config.ShortTx {
		key1 := t.Tx.EthTx.From().String()
		key2 := t.Tx.EthTx.SmallBankTo.String()
		t.Tx.Vertex.WriteKeys[key1] = "value"
		t.Tx.Vertex.WriteKeys[key2] = "value"

		t.Tx.Vertex.ReadKeys[key2] = "value"
		t.Tx.Vertex.ReadKeys[key2] = "value"
	} else {
		key1 := t.Tx.EthTx.SmallBankTo.String()
		t.Tx.Vertex.WriteKeys[key1] = "value"
		t.Tx.Vertex.ReadKeys[key1] = "value"
	}
}
