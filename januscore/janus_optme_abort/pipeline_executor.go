package janusOptmeAbort

import (
	"Janus/ethereum/core/vm"

	//"Janus/ethereum/core/types"
	"Janus/tools"

	"github.com/holiman/uint256"
)

// executeTransaction 执行交易
func (pe *PipelineEngine) executeTransaction(jtx *janusTransaction, workerID int) {
	tx := jtx.Tx

	// 真实以太坊负载不再进入 EVM：按 LatencyDB 的 latency 忙等，并直接使用 LatencyDB 读写集。
	// 合成负载保持原来的 EVM 执行和 opcode cost 统计。
	if tools.ExecuteSimulatedTransaction(tx) {
		jtx.ExecutionCost = tools.SimulatedTransactionCost(tx)
	} else {
		vm.OpenTxCost(workerID)
		//fmt.Println("inputs:", common.Bytes2Hex(tx.Data()))
		_, err := pe.levms[workerID].CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
		tools.PanicError("Janus Tx Execute ", err)
		jtx.ExecutionCost = vm.CloseTxCost(workerID)
	}
	readSet, writeSet := tools.TransactionReadWriteSet(tx)

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
	tx := oldRWSet.Tx.Tx

	// 重执行阶段同样走模拟分支，避免 abort 后重新进入真实 EVM 导致执行失败。
	if !tools.ExecuteSimulatedTransaction(tx) {
		_, err := pe.levms[workerID].CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
		tools.PanicError("Janus Tx ReExecute", err)
	}
	readSet, writeSet := tools.TransactionReadWriteSet(tx)

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
