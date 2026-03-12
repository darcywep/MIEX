package janusHarmonyAbort

import (
	janusConfig "Janus/config"
	"Janus/ethereum/core/vm"

	//"Janus/ethereum/core/types"
	"Janus/tools"

	"github.com/holiman/uint256"
)

// executeTransaction 执行交易
func (pe *PipelineEngine) executeTransaction(jtx *janusTransaction, workerID int) {
	readSet := make(map[string]struct{})
	writeSet := make(map[string]struct{})
	tx := jtx.Tx

	vm.OpenTxCost(workerID)
	//fmt.Println("inputs:", common.Bytes2Hex(tx.Data()))
	_, err := pe.levms[workerID].CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("Janus Tx Execute ", err)
	jtx.ExecutionCost = vm.CloseTxCost(workerID)

	if tx.TxType == janusConfig.ShortTx {
		key1 := tx.From().String()
		key2 := tx.SmallBankTo.String()
		writeSet[key1] = struct{}{}
		writeSet[key2] = struct{}{}
		readSet[key1] = struct{}{}
		readSet[key2] = struct{}{}
	} else {
		key1 := tx.SmallBankTo.String()
		writeSet[key1] = struct{}{}
		readSet[key1] = struct{}{}
	}

	jtx.rwSet = &ReadWriteSet{
		TxID:       jtx.OriginalIdx,
		Tx:         jtx,
		ReadSet:    readSet,
		WriteSet:   writeSet,
		Cost:       jtx.ExecutionCost,
		ThreadID:   workerID,
		EarlyAbort: false,
		Executed:   false,
	}
}

// reExecuteTransaction 重执行交易
func (pe *PipelineEngine) reExecuteTransaction(oldRWSet *ReadWriteSet, workerID int) *ReadWriteSet {
	readSet := make(map[string]struct{})
	writeSet := make(map[string]struct{})
	tx := oldRWSet.Tx.Tx

	_, err := pe.levms[workerID].CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("Janus Tx ReExecute", err)

	if tx.TxType == janusConfig.ShortTx {
		key1 := tx.From().String()
		key2 := tx.SmallBankTo.String()
		writeSet[key1] = struct{}{}
		writeSet[key2] = struct{}{}
		readSet[key1] = struct{}{}
		readSet[key2] = struct{}{}
	} else {
		key1 := tx.SmallBankTo.String()
		writeSet[key1] = struct{}{}
		readSet[key1] = struct{}{}
	}

	rwSet := &ReadWriteSet{
		TxID:       oldRWSet.TxID,
		Tx:         oldRWSet.Tx,
		ReadSet:    readSet,
		WriteSet:   writeSet,
		Cost:       oldRWSet.Tx.ExecutionCost,
		ThreadID:   workerID,
		EarlyAbort: false,
		Executed:   false,
	}

	oldRWSet.Tx.rwSet = rwSet

	return rwSet
}
