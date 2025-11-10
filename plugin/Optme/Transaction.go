package Optme

import (
	"Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/plugin/Common"
	"Janus/tools"

	"github.com/holiman/uint256"

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

	EthTx *types.Transaction
}

func NewOptmeTransaction(tx *Common.JanusTransaction, EthTx *types.Transaction, blockid uint32) *OptmeTransaction {
	return &OptmeTransaction{Tx: tx, Blockid: blockid, EthTx: EthTx, LocalGet: make(map[string]string), LocalPut: make(map[string]string)}
}

// 即将换成EVM逻辑
func (t *OptmeTransaction) Execute(levm *lvm.LEVM) {

	//tools.CatStorageState = true
	_, err := levm.CallContract(*t.EthTx.From(), *t.EthTx.To(), t.EthTx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("OptmeTransaction Execute", err)

	//tx.WriteKeys = make([]string, 0)
	//tx.ReadKeys = make([]string, 0)
	if t.EthTx.TxType == config.IOTx {

		key1 := t.EthTx.From().String()
		key2 := t.EthTx.SmallBankTo.String()
		t.Tx.Vertex.WriteKeys[key1] = "value"
		t.Tx.Vertex.WriteKeys[key2] = "value"

		t.Tx.Vertex.ReadKeys[key2] = "value"
		t.Tx.Vertex.ReadKeys[key2] = "value"

		//tx.WriteKeys = append(tx.WriteKeys, tx.From().String())
		//tx.WriteKeys = append(tx.WriteKeys, tx.SmallBankTo.String())
	} else {
		//tx.WriteKeys = append(tx.ReadKeys, tx.SmallBankTo.String())
		key1 := t.EthTx.SmallBankTo.String()
		t.Tx.Vertex.WriteKeys[key1] = "value"

		t.Tx.Vertex.ReadKeys[key1] = "value"

	}
}
