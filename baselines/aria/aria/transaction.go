package aria

import (
	janusCommon "Janus/baselines/common"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
)

type AriaTransaction struct {
	Inner        janusCommon.JanusTransaction
	EthTx        *types.Transaction
	ID           uint64
	BatchID      uint64
	LocalGet     map[string]string
	LocalPut     map[string]string
	StartTime    time.Time
	flagConflict atomic.Bool
	committed    atomic.Uint32
}

func NewAriaTransaction(inner janusCommon.JanusTransaction, ethTx *types.Transaction, id, batch uint64) *AriaTransaction {
	return &AriaTransaction{
		Inner:    inner,
		EthTx:    ethTx,
		ID:       id,
		BatchID:  batch,
		LocalGet: make(map[string]string),
		LocalPut: make(map[string]string),
	}
}

func (tx *AriaTransaction) Execute(levm *lvm.LEVM) {
	//fmt.Println("tx Execute:", tx.ID)
	_, err := levm.CallContract(*tx.EthTx.From(), *tx.EthTx.To(), tx.EthTx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("AriaTransaction Execute", err)
}
func (tx *AriaTransaction) CountOverheads() uint32 { return tx.Inner.Cost }

func (tx *AriaTransaction) SetConflict(v bool) {
	if v {
		tx.flagConflict.Store(true)
	} else {
		tx.flagConflict.Store(false)
	}
}
func (tx *AriaTransaction) HasConflict() bool { return tx.flagConflict.Load() }

func (tx *AriaTransaction) SetCommitted(v bool) {
	if v {
		tx.committed.Store(1)
	} else {
		tx.committed.Store(0)
	}
}
func (tx *AriaTransaction) IsCommitted() bool {
	return tx.committed.Load() != 0
}
