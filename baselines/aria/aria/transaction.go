package aria

import (
	janusCommon "Janus/baselines/common"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/vm"
	"Janus/tools"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
)

type AriaTransaction struct {
	Inner           janusCommon.BasicTransaction
	OriginalBlockID int
	OriginalTxID    int
	ID              uint64
	BatchID         uint64
	LocalGet        map[string]string
	LocalPut        map[string]string
	ExecutionCost   float64
	StartTime       time.Time
	flagConflict    atomic.Bool
	committed       atomic.Uint32
}

func NewAriaTransaction(inner janusCommon.BasicTransaction, id, batch uint64, OriginalBlockID, OriginalTxID int) *AriaTransaction {
	return &AriaTransaction{
		Inner:           inner,
		ID:              id,
		BatchID:         batch,
		OriginalBlockID: OriginalBlockID,
		OriginalTxID:    OriginalTxID,
		LocalGet:        make(map[string]string),
		LocalPut:        make(map[string]string),
	}
}

func (tx *AriaTransaction) Execute(levm *lvm.LEVM, workerID int) {
	if tools.ExecuteSimulatedTransaction(tx.Inner.EthTx) {
		tx.ExecutionCost = tools.SimulatedTransactionCost(tx.Inner.EthTx)
		return
	}
	vm.OpenTxCost(workerID)
	_, err := levm.CallContract(*tx.Inner.EthTx.From(), *tx.Inner.EthTx.To(), tx.Inner.EthTx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("AriaTransaction Execute", err)
	tx.ExecutionCost = vm.CloseTxCost(workerID)
}

func (tx *AriaTransaction) SetConflict(v bool) {
	tx.flagConflict.Store(v)
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
