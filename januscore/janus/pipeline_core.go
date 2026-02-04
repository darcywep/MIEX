package janus

import (
	lvm "Janus/core/evm"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
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
	start := time.Now()
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
			allTxs:                 batch.AllTxs,
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
	pe.timeOfSubmitBlock += time.Since(start)
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
			pe.executeCurrentBatch(state, workerID)
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
		pe.executeTransaction((*txs)[idx], workerID)
	}
}

// executeCurrentBatch 执行当前批次的交易
// 优先级：长交易 > 短交易 > 下一批交易
// pairWorkerID 返回配对的工作线程ID，用于构建DAG
func (pe *PipelineEngine) executeCurrentBatch(state *BatchState, workerID int) (pairStateTable *stateTableWithWriteSet, isMerge bool) {
	//// ========= 1. 优先执行长交易 =========
	//pe.executeNextTransaction(&state.LongTxIndex, &state.LongTxs, workerID, state)

	// ========= 2. 执行短交易 =========
	//pe.executeNextTransaction(&state.ShortTxIndex, &state.ShortTxs, workerID, state)
	pe.executeNextTransaction(&state.ShortTxIndex, &state.allTxs, workerID, state)

	// ========= 3. 当前批次没有交易可以执行, 尝试部分线程进行下一批的执行 =========

	// 说明：这里设计成部分线程进入下一批执行，是为了避免所有线程都进入下一批，导致当前批次无法完成，从而阻塞流水线
	finishedNumber := state.finishedNumber.Add(1)
	if int(finishedNumber) < pe.numThreads { // 只需保留一个线程
		pe.workerStaties[workerID].Phase = WaitingPhase
		return nil, false
	}

	// 处理提交
	// TODO: 进入下一阶段
	pe.workerStaties[workerID].Phase = WaitingPhase
	ok := state.finished.CompareAndSwap(false, true)
	if ok {
		pe.currentBatchID.Add(1)
		pe.completeBatch(state) // 完成该批次
	}
	// 部分线程可以切换到下一批

	return nil, false
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
