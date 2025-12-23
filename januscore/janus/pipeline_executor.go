package janus

import (
	janusConfig "Janus/config"
	"fmt"

	//"Janus/ethereum/core/types"
	"Janus/tools"

	"github.com/holiman/uint256"
)

// executeTask 执行任务
func (pe *PipelineEngine) executeTask(task *Task, workerID int) {
	//fmt.Printf("Executing WorkerID=%d, TxIndex=%d, BatchID=%d\n", workerID, task.Tx.OriginalIdx, task.BatchID)
	//time.Sleep(2 * time.Second) // 模拟执行时间
	switch task.Type {
	case TaskExecLong, TaskExecShort:
		// 执行当前批次交易
		rwset := pe.executeTransaction(task, workerID)

		peCurrentBatchID := pe.currentBatchID.Load()
		state := pe.batchStates[peCurrentBatchID]

		if state.BatchID == task.BatchID { // 确认仍在当前批次
			// 收集结果
			if state.ThreadRWSets[workerID] == nil {
				state.ThreadRWSets[workerID] = make([]*ReadWriteSet, 0)
			}
			state.ThreadRWSets[workerID] = append(state.ThreadRWSets[workerID], rwset)

			// 增加完成计数
			completed := state.ExecCompleted.Add(1)
			if int(completed) == state.TotalTxs {
				fmt.Printf("[Execution] Batch %d: all %d transactions executed\n",
					state.BatchID, state.TotalTxs)
			}
		} else {
			// 已切换批次，结果丢弃
			fmt.Printf("Error [Worker %d] Discarding result of tx %d from old batch %d (current batch %d)\n",
				workerID, task.TxID, task.BatchID, peCurrentBatchID)
		}

	case TaskExecNext:
		// 执行下一批交易（水位线间并发）
		pe.executeTransaction(task, workerID)
	}
}

// executeTransaction 执行交易
func (pe *PipelineEngine) executeTransaction(task *Task, workerID int) *ReadWriteSet {
	readSet := make(map[string]struct{})
	writeSet := make(map[string]struct{})
	tx := task.Tx.Tx

	_, err := pe.levms[workerID].CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("Janus Tx Execute", err)

	if tx.TxType == janusConfig.IOTx {
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
		TxID:       task.TxID,
		Tx:         task.Tx,
		ReadSet:    readSet,
		WriteSet:   writeSet,
		Cost:       tx.ExecutionCost,
		ThreadID:   workerID,
		EarlyAbort: false,
		Executed:   false,
	}

	task.Tx.rwSet = rwSet

	return rwSet
}

// reExecuteTransaction 重执行交易
func (pe *PipelineEngine) reExecuteTransaction(oldRWSet *ReadWriteSet, workerID int) *ReadWriteSet {
	readSet := make(map[string]struct{})
	writeSet := make(map[string]struct{})
	tx := oldRWSet.Tx.Tx

	_, err := pe.levms[workerID].CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("Janus Tx ReExecute", err)

	if tx.TxType == janusConfig.IOTx {
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
		Cost:       tx.ExecutionCost,
		ThreadID:   workerID,
		EarlyAbort: false,
		Executed:   false,
	}

	oldRWSet.Tx.rwSet = rwSet

	return rwSet
}

// hasConflictWithBatch 检查与当前批次的冲突
func (pe *PipelineEngine) hasConflictWithBatch(state *BatchState, rwset *ReadWriteSet) bool {
	//state.ExecResultsMu.Lock()
	//defer state.ExecResultsMu.Unlock()
	//
	//for _, existRWSet := range state.ExecResults {
	//	if pe.hasConflict(rwset, existRWSet) {
	//		return true
	//	}
	//}
	return false
}

// hasConflict 检查两个交易是否有冲突
func (pe *PipelineEngine) hasConflict(rw1, rw2 *ReadWriteSet) bool {
	// Write-Write 冲突
	for key := range rw1.WriteSet {
		if _, exists := rw2.WriteSet[key]; exists {
			return true
		}
	}

	// Read-Write 冲突
	for key := range rw1.ReadSet {
		if _, exists := rw2.WriteSet[key]; exists {
			return true
		}
	}

	// Write-Read 冲突
	for key := range rw1.WriteSet {
		if _, exists := rw2.ReadSet[key]; exists {
			return true
		}
	}

	return false
}
