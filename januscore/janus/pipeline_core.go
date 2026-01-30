package janus

import (
	lvm "Janus/core/evm"
	"fmt"
	"runtime"
	"sort"
	"sync/atomic"
)

// NewPipelineEngine 创建流水线引擎
func NewPipelineEngine(levm *lvm.LEVM, numThreads int) *PipelineEngine {
	levms := make([]*lvm.LEVM, numThreads)
	for i := 0; i < numThreads; i++ {
		levms[i] = levm.Copy()
		levms[i].SetEVMWorkerID(i)
	}
	pl := &PipelineEngine{
		levms:         levms,
		numThreads:    numThreads,
		workerStaties: make([]*WorkerStats, numThreads),
		stopChan:      make(chan struct{}),
		completeChan:  make(chan int, 100),
	}
	pl.currentBatchID.Store(-1)
	pl.currentBlockID.Store(-1)
	return pl
}

// Start 启动引擎
func (pe *PipelineEngine) Start() {
	// 启动工作线程（包含批次管理逻辑）
	for i := 0; i < pe.numThreads; i++ {
		pe.workerWg.Add(1)
		pe.workerStaties[i] = NewWorkerStats(i)
		go pe.workerThread(i)
	}
}

// Stop 停止引擎
func (pe *PipelineEngine) Stop() {
	pe.stopChan <- struct{}{}
	close(pe.stopChan)
	close(pe.completeChan)
	pe.workerWg.Wait()
}

// SubmitBlockBatches 提交一个区块的所有批次
func (pe *PipelineEngine) SubmitBlockBatches(batches []*Batch, jtxs []*janusTransaction) {
	pe.janusTransactions = jtxs
	// 创建批次状态
	pe.batchStates = make([]*BatchState, len(batches))
	pe.currentBatchID.Store(-1)

	for i, batch := range batches {
		var nextBatch *Batch = nil
		if i+1 < len(batches) {
			nextBatch = batches[i+1]
		}

		state := &BatchState{
			BatchID:                batch.ID,
			Batch:                  batch,
			NextBatch:              nextBatch,
			LongTxs:                batch.LongTxs,
			ShortTxs:               batch.ShortTxs,
			writeSet:               make(map[string]struct{}),
			ThreadRWSets:           make([][]*ReadWriteSet, pe.numThreads),
			ThreadStateTables:      make([]*stateTableWithWriteSet, pe.numThreads),
			MergeThreadStateTables: newThreadStateTableForMerge(pe.numThreads),
			constructDAG:           newConstructDAGResult(pe.numThreads),
			CommittedTxs:           make([]int, 0),
			reExecute:              nil,
			TotalTxs:               len(batch.AllTxs),
		}

		pe.batchStates[i] = state // 设置批次切片
		if i == 0 {
			pe.currentBlockID.Add(1)
			pe.currentBatchID.Store(0) // 重置为 -1，第一次加载时会变成 0
			//fmt.Printf("pe.currentBatchID.Load()=%d\n", pe.currentBatchID.Load())
		}
	}
	//fmt.Printf("[SubmitBlock] Submitted %d batches for execution\n", len(batches))
}

// workerThread 工作线程（STM 风格 + 批次管理）
func (pe *PipelineEngine) workerThread(workerID int) {
	defer pe.workerWg.Done()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	for {
		select {
		case <-pe.stopChan:
			return
		default:
		}
		currentBlockID := pe.currentBlockID.Load()
		if currentBlockID == -1 { // 等待区块
			continue
		}
		if currentBlockID > pe.workerStaties[workerID].CurrentBlockID {
			if enableLog {
				//fmt.Printf("Worker %d is already running, stateId=%d, batch length=%d\n", workerID, stateID, len(pe.batchStates))
			}
			pe.workerStaties[workerID].CurrentBlockID = currentBlockID
			pe.workerStaties[workerID].currentBatchID = -1
			pe.workerStaties[workerID].Phase = WaitingPhase
		}
		stateID := pe.currentBatchID.Load()
		if stateID >= int32(len(pe.batchStates)) {
			continue
		}

		if stateID == -1 { // 尚未有批次
			continue
		}
		// 切换到新的批次状态
		if stateID > pe.workerStaties[workerID].currentBatchID {
			pe.workerStaties[workerID].currentBatchID = stateID
			pe.workerStaties[workerID].Phase = ExecuteCurrentBatchPhase
			if enableLog {
				fmt.Printf("[Worker %d] Switched to Batch %d\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
		}

		state := pe.batchStates[stateID]
		if state == nil {
			continue
		}
		if pe.workerStaties[workerID].Phase == ExecuteCurrentBatchPhase {
			if enableLog {
				fmt.Printf("[Worker %d] entry execute current batch(%d) phase.\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
			pairStateTable, isMerge := pe.executeCurrentBatch(state, workerID)
			var mergeThreadStateTable *stateTableWithWriteSet = state.ThreadStateTables[workerID]
			for isMerge {
				mergeThreadStateTable, pairStateTable, isMerge = pe.mergeStateTables(state, mergeThreadStateTable, pairStateTable, workerID)
			}
			if enableLog {
				fmt.Printf("[Worker %d] finished execute current batch(%d) phase.\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
		}

		if pe.workerStaties[workerID].Phase == PreExecuteNextBatchPhase {
			if enableLog {
				fmt.Printf("[Worker %d] entry pre execute next batch(%d) phase.\n", workerID, pe.workerStaties[workerID].currentBatchID+1)
			}
			pe.executeNextBatch(state, workerID)
			continue // 不能直接往下，因为可能当前批次已完成
		}
	}
}

func (pe *PipelineEngine) executeNextTransaction(atomicIdx *atomic.Int32, txs *[]*janusTransaction, workerID int, state *BatchState) {
	// 循环尝试从队列中抢任务
	for {
		idx := int(atomicIdx.Add(1) - 1) // 下标从0开始的
		if idx >= len(*txs) {            // 没有交易可以执行
			return
		}

		jtx := (*txs)[idx]
		var needRun = false
		if jtx.IsRuned { // 如果已经执行过, 需要检查是否要重新执行
			preState := pe.batchStates[state.BatchID-1] // 如果已经执行过，那么之前必定有一个批次
			for readKey, _ := range jtx.rwSet.ReadSet {
				if _, exist := preState.writeSet[readKey]; exist { // 读集和上一批的写集冲突，重执行
					needRun = true // 需要重新执行
					break
				}
			}
		} else { // 未曾执行过
			needRun = true
		}

		//if enableLog { // 打印日志信息
		//	if jtx.IsRuned && needRun {
		//		fmt.Printf("[Worker %d] [batch %d] tx %d already runed, but have conflict with pre batch, need re-execute.\n", workerID, state.BatchID, jtx.OriginalIdx)
		//	} else if !jtx.IsRuned {
		//		fmt.Printf("[Worker %d] [batch %d] tx %d need run.\n", workerID, state.BatchID, jtx.OriginalIdx)
		//	} else if !needRun {
		//		fmt.Printf("[Worker %d] [batch %d] tx %d already runed, and not need run.\n", workerID, state.BatchID, jtx.OriginalIdx)
		//	}
		//}

		if !needRun { // 不需要执行, 但仍然需要将读写集放入到线程的读写集表中
			appendThreadRWSets(state, jtx, workerID)
			continue
		}

		// 需要执行交易
		jtx.IsRuned = true

		pe.executeTransaction(jtx, workerID)
		appendThreadRWSets(state, jtx, workerID)
	}
}

// executeCurrentBatch 执行当前批次的交易
// 优先级：长交易 > 短交易 > 下一批交易
// pairWorkerID 返回配对的工作线程ID，用于构建DAG
func (pe *PipelineEngine) executeCurrentBatch(state *BatchState, workerID int) (pairStateTable *stateTableWithWriteSet, isMerge bool) {
	// ========= 1. 优先执行长交易 =========
	pe.executeNextTransaction(&state.LongTxIndex, &state.LongTxs, workerID, state)

	// ========= 2. 执行短交易 =========
	pe.executeNextTransaction(&state.ShortTxIndex, &state.ShortTxs, workerID, state)

	// ========= 3. 当前批次没有交易可以执行, 尝试部分线程进行下一批的执行 =========

	// 说明：这里设计成部分线程进入下一批执行，是为了避免所有线程都进入下一批，导致当前批次无法完成，从而阻塞流水线
	finishedNumber := state.finishedNumber.Add(1)
	if int(finishedNumber) < pe.numThreads-1 { // 只需保留一个线程
		if state.NextBatch != nil {
			pe.workerStaties[workerID].Phase = PreExecuteNextBatchPhase // 成功增加线程数
		} else { // 如果没有下一批, 即最后一批，那么直接进入等待阶段，等待本批次完成后进入下一区块
			pe.workerStaties[workerID].Phase = WaitingPhase
		}
		return nil, false
	}

	// 处理提交
	// TODO: 每个批次先提交开销最大的节点
	// 对 LongTxs 按 ExecutionCost 从大到小排序
	sort.Slice(state.LongTxs, func(i, j int) bool { return state.LongTxs[i].ExecutionCost > state.LongTxs[j].ExecutionCost })

	// 对 ShortTxs 按 ExecutionCost 从大到小排序
	sort.Slice(state.ShortTxs, func(i, j int) bool { return state.ShortTxs[i].ExecutionCost > state.ShortTxs[j].ExecutionCost })

	//for i := 0; i < len(state.LongTxs); i++ {
	//	if pe.workerStaties[workerID].Phase ==
	//}

	// 部分线程可以切换到下一批

	return nil, false
}

// executeNextBatch 执行下一批次的交易
// 优先级：长交易 > 短交易
// pairWorkerID 返回配对的工作线程ID，用于构建DAG
func (pe *PipelineEngine) executeNextBatch(state *BatchState, workerID int) {
	executeNextBatchTransaction := func(atomicIdx *atomic.Int32, txs *[]*janusTransaction) (isNewBatch bool) {
		// 循环尝试从队列中抢任务
		for {
			if state.finished.Load() { // 如果本批次执行完成，则停止执行
				return true
			}

			idx := int(atomicIdx.Add(1) - 1) // 下标从0开始的
			if idx >= len(*txs) {            // 没有交易可以执行
				return false
			}

			pe.executeTransaction((*txs)[idx], workerID)
			(*txs)[idx].IsRuned = true // 必须标注

			if state.finished.Load() { // 如果已经切换批次，则停止执行
				return true
			}
		}
	}

	// 1. 执行下一批长交易（水位线间并发）
	executeNextBatchTransaction(&state.NextLongTxIndex, &state.NextBatch.LongTxs)

	// 2. 执行下一批短交易（水位线间并发）
	isNewBatch := executeNextBatchTransaction(&state.NextShortTxIndex, &state.NextBatch.ShortTxs)
	if isNewBatch { // 如果已经切换批次，则停止执行
		pe.workerStaties[workerID].Phase = WaitingPhase
		//if enableLog {
		//fmt.Printf("[WARNNING] [Worker %d] worker now is executing next batch, but pipeline engine was entried next batch(%d), "+
		//	"this worker is entring waiting phase.\n", workerID, pe.currentBatchID.Load())
		//}
		return
	}

	// 已经完成下一批的预执行，且没有正式进入下一批, 那么就进入下一阶段
	pe.workerStaties[workerID].Phase = ConstructDAGPhase
	if enableLog {
		fmt.Printf("[Worker %d] Batch %d is not next batch or next batch is all executed, tring entry construct DAG phase.\n", workerID, state.BatchID)
	}
}

// WaitForBlockCompletion 等待一个区块的所有批次完成
func (pe *PipelineEngine) WaitForBlockCompletion(batchCount int) {

	for i := 0; i < batchCount; i++ {
		<-pe.completeChan
	}
}

// completeBatch 完成批次
func (pe *PipelineEngine) completeBatch(state *BatchState) {
	pe.completeChan <- state.BatchID
}

// compareRWSet 比较两个读写集是否相同（只比较涉及冲突的部分）
func (pe *PipelineEngine) compareRWSet(old, new *ReadWriteSet) bool {
	// 比较读集
	if len(old.ReadSet) != len(new.ReadSet) {
		return false
	}
	for addr := range new.ReadSet { // 新的在老的里面没有，那么就需要重执行
		if _, exists := old.ReadSet[addr]; !exists {
			return false
		}
	}

	// 比较写集
	if len(old.WriteSet) != len(new.WriteSet) {
		return false
	}
	for addr := range new.WriteSet {
		if _, exists := old.WriteSet[addr]; !exists {
			return false
		}
	}

	return true
}
