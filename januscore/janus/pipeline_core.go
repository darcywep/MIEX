package janus

import (
	lvm "Janus/core/evm"
	"fmt"
	"runtime"
	"sync/atomic"
)

// NewPipelineEngine 创建流水线引擎
func NewPipelineEngine(levm *lvm.LEVM, numThreads int) *PipelineEngine {
	levms := make([]*lvm.LEVM, numThreads)
	for i := 0; i < numThreads; i++ {
		levms[i] = levm.Copy()
	}
	pl := &PipelineEngine{
		levms:         levms,
		numThreads:    numThreads,
		workerStaties: make([]*WorkerStats, numThreads),
		stopChan:      make(chan struct{}),
		completeChan:  make(chan int, 100),
	}
	pl.currentBatchID.Store(-1)
	return pl
}

// Start 启动引擎
func (pe *PipelineEngine) Start() {
	fmt.Printf("=== Starting %d worker threads ===\n", pe.numThreads)

	// 启动工作线程（包含批次管理逻辑）
	for i := 0; i < pe.numThreads; i++ {
		pe.workerWg.Add(1)
		pe.workerStaties[i] = NewWorkerStats(i)
		go pe.workerThread(i)
	}
}

// Stop 停止引擎
func (pe *PipelineEngine) Stop() {
	close(pe.stopChan)
	pe.workerWg.Wait()
}

// SubmitBlockBatches 提交一个区块的所有批次
func (pe *PipelineEngine) SubmitBlockBatches(batches []*Batch, jtxs []*janusTransaction) {
	pe.janusTransactions = jtxs
	// 创建批次状态
	pe.batchStates = make([]*BatchState, len(batches))

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
			ThreadRWSets:           make([][]*ReadWriteSet, pe.numThreads),
			ThreadStateTables:      make([]map[string]*StateTable, pe.numThreads),
			MergeThreadStateTables: newThreadStateTableForMerge(pe.numThreads),
			constructDAG:           newConstructDAGResult(pe.numThreads),
			CompletionOrder:        make([]int, 0),
			TotalTxs:               len(batch.AllTxs),
		}

		pe.batchStates[i] = state // 设置批次切片
		if i == 0 {
			pe.currentBatchID.CompareAndSwap(-1, 0) // 重置为 -1，第一次加载时会变成 0
			fmt.Println("pe.currentBatchID.Load()", pe.currentBatchID.Load())
		}
	}

	fmt.Printf("[SubmitBlock] Submitted %d batches for execution\n", len(batches))
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

		if pe.currentBatchID.Load() == -1 { // 尚未有批次
			continue
		}
		// 切换到新的批次状态
		if pe.currentBatchID.Load() > pe.workerStaties[workerID].currentBatchID {
			pe.workerStaties[workerID].currentBatchID = pe.currentBatchID.Load()
			pe.workerStaties[workerID].Phase = ExecuteTaskPhase
			fmt.Printf("[Worker %d] Switched to Batch %d\n", workerID, pe.workerStaties[workerID].currentBatchID)
		}

		state := pe.batchStates[pe.currentBatchID.Load()]
		if pe.workerStaties[workerID].Phase == ExecuteTaskPhase {
			task, pairWorkerID := pe.nextTask(workerID)
			if task != nil {
				// 执行任务
				pe.executeTask(task, workerID)
				continue
			}
			// task == nil
			if pairWorkerID >= 0 { // 有配对任务
				// 没有可执行的任务
				// 1. 配对构建StateTable
				mergeThreadStateTable, pairStateTable := pe.mergeStateTables(state, state.ThreadStateTables[workerID], state.ThreadStateTables[pairWorkerID], workerID)
				for pairStateTable != nil {
					mergeThreadStateTable, pairStateTable = pe.mergeStateTables(state, mergeThreadStateTable, pairStateTable, workerID)
				}
				// 2. 继续循环获取任务，进入 MergeStateTablePhase 阶段，不会继续nextTask
				continue
			}
			if pairWorkerID == -1 {
				// 无任务可执行，继续循环获取任务
				pe.workerStaties[workerID].Phase = ConstructDAGPhase
				continue
			}
		}

		if pe.workerStaties[workerID].Phase == ConstructDAGPhase {
			for !state.MergeThreadStateTables.done.Load() {
				// busy wait
			}
			// 尝试获取合并任务
			pairDag := pe.tryConstructDAG(state, workerID)
			for pairDag != nil {
				pairDag = pe.tryMergeDag(state, pairDag, workerID)
			}
			continue
		}
		if pe.workerStaties[workerID].Phase == CommitMaximumValidationPhase {

		}
	}
}

func (pe *PipelineEngine) searchNextTask(atomicIdx *atomic.Int32, txs []*janusTransaction, workerID int, state *BatchState) *Task {
	// 循环尝试从队列中抢任务
	for {
		idx := int(atomicIdx.Load())
		if idx >= len(txs) {
			return nil
		}

		// 抢一个 slot
		if atomicIdx.CompareAndSwap(int32(idx), int32(idx+1)) {
			jtx := txs[idx]

			// 如果已经执行过且非 early abort，就跳过并继续循环
			if jtx.IsRuned && !jtx.EarlyAbort {
				fmt.Printf("[Worker %d] Skip tx %d (already runed and not early aborted)\n",
					workerID, jtx.OriginalIdx)
				state.ExecCompleted.Add(1)
				continue
			}

			jtx.IsRuned = true
			// 正常任务
			return &Task{
				Type:    TaskExecLong,
				BatchID: state.BatchID,
				TxID:    jtx.OriginalIdx,
				Tx:      jtx,
			}
		}

		// 抢失败，继续循环重试
	}
}

func (pe *PipelineEngine) searchNextBatchTask(atomicIdx *atomic.Int32, txs []*janusTransaction, workerID int, state *BatchState) *Task {
	for {
		idx := int(atomicIdx.Load())
		if idx >= len(txs) {
			return nil // 队列已完成
		}

		// 抢占成功
		if atomicIdx.CompareAndSwap(int32(idx), int32(idx+1)) {
			jtx := txs[idx]
			//fmt.Printf("[Worker %d] Switched to Executing Next Batch %d, TxID %d\n", workerID, state.BatchID, jtx.OriginalIdx)
			jtx.IsRuned = true
			return &Task{
				Type:    TaskExecNext,
				BatchID: state.BatchID + 1,
				TxID:    jtx.OriginalIdx,
				Tx:      jtx,
			}
		}

		// 抢占失败，继续尝试
		// （这里不 sleep/backoff，保持你的原逻辑）
	}
}

// nextTask 获取下一个任务（STM 风格）
// 优先级：长交易 > 短交易 > 下一批交易
// pairWorkerID 返回配对的工作线程ID，用于构建DAG
func (pe *PipelineEngine) nextTask(workerID int) (task *Task, pairWorkerID int) {
	peCurrentBatchID := pe.currentBatchID.Load() // 仍然处于当前批次
	state := pe.batchStates[peCurrentBatchID]
	if peCurrentBatchID == pe.workerStaties[workerID].currentBatchID { // 尚未切换批次
		// 1. 优先执行长交易
		task = pe.searchNextTask(&state.LongTxIndex, state.LongTxs, workerID, state)
		if task != nil { // 找到任务
			return task, -1
		}

		// 2. 执行短交易
		task = pe.searchNextTask(&state.ShortTxIndex, state.ShortTxs, workerID, state)
		if task != nil { // 找到任务
			return task, -1
		}

		// 无法找到该批次的执行任务了，尝试部分线程进行下一批的执行
		// 线程完成本批次的任务后，需要先构建自己的状态读写表
		if state.ThreadStateTables[workerID] == nil {
			state.ThreadStateTables[workerID] = pe.buildStateTable(state.ThreadRWSets[workerID])
		}

		// 3. 尝试进入下一批执行（水位线间并发）
		// 说明：这里设计成部分线程进入下一批执行，是为了避免所有线程都进入下一批，导致当前批次无法完成，从而阻塞流水线
		for {
			state.CompletionMu.Lock()
			threadNextNumber := len(state.CompletionOrder) // 有多少线程已经完成当前批次并并已经进入下一批
			if threadNextNumber >= (pe.numThreads+1)/2 {   // 最多允许一半线程切换，向上取整
				pairIndex := state.pairIndex
				state.pairIndex++
				state.CompletionMu.Unlock()

				if pairIndex == 0 && pe.numThreads%2 == 1 { // 奇数线程时，最后一个线程无需配对
					lastWorkerID := state.CompletionOrder[threadNextNumber-1]
					state.MergeThreadStateTables.stateTablesQueue = append(state.MergeThreadStateTables.stateTablesQueue, state.ThreadStateTables[lastWorkerID])
				}
				return nil, state.CompletionOrder[pairIndex] // 返回配对的线程ID
			}

			// 部分线程可以切换到下一批
			state.CompletionOrder = append(state.CompletionOrder, workerID) // 先加入完成队列，防止竞争
			state.CompletionMu.Unlock()
			if state.NextBatch != nil {
				pe.workerStaties[workerID].currentBatchID++ // 成功增加线程数
			}
			break
		}
	}

	// 部分线程已切换到新批次
	if peCurrentBatchID < pe.workerStaties[workerID].currentBatchID {
		// 2. 执行下一批长交易（水位线间并发）
		task = pe.searchNextBatchTask(&state.NextLongTxIndex, state.NextBatch.LongTxs, workerID, state)
		if task != nil {
			return task, -1
		}

		// 3. 执行下一批短交易（水位线间并发）
		task = pe.searchNextBatchTask(&state.NextShortTxIndex, state.NextBatch.ShortTxs, workerID, state)
		if task != nil {
			return task, -1
		}
	}

	// 无任务可执行
	// 1. 下一批也无任务，或无下一批
	fmt.Printf("[Worker %d]  Batch %d: Not Next Batch or Next Batch is all Executed,  waiting...\n", workerID, state.BatchID)

	//startWait := time.Now()
	//isWait := false
	//for !state.MergeThreadStateTables.done.Load() {
	//	isWait = true
	//}
	////state.MergeThreadStateTables.condMu.Lock()
	////for !state.MergeThreadStateTables.done {
	////	isWait = true
	////	state.MergeThreadStateTables.cond.Wait()
	////}
	////state.MergeThreadStateTables.condMu.Unlock()
	//elapsed := time.Since(startWait)
	//fmt.Printf("[Woker %d] Batch %d: Waiting %t, Entry new phase resumed after waiting %s\n", workerID, state.BatchID, isWait, elapsed)
	return nil, -1
}

//func (pe *PipelineEngine) tryConstructDAG(state *BatchState, workerID int) (pairDag *ConflictDAG) {
//	// 循环尝试从队列中抢任务
//	for {
//		idx := int(state.constructDAG.stateTableIndex.Load())
//		if idx >= len(state.constructDAG.stateTables) {
//			if state.constructDAG.dags[workerID] == nil { // 当前线程没有构建任何 DAG，说明没有任务可做，等待
//				fmt.Printf("[Worker %d] Batch %d: New join thread, but no more StateTables to construct DAG, waiting...\n", workerID, state.BatchID)
//				startWait := time.Now()
//				isWait := waitHere(workerID, &state.constructDAG.condMu, state.constructDAG.cond, &state.constructDAG.done)
//				elapsed := time.Since(startWait)
//				fmt.Printf("[Woker %d] Batch %d: New join thread, waiting %t, entry new phase resumed after waiting %s\n", workerID, state.BatchID, isWait, elapsed)
//				pe.workerStaties[workerID].Phase = CommitMaximumValidationPhase // 被唤醒之后进入下一阶段
//
//			} else { // 当前线程已经构建了 DAG，需要判断是否为第一个完成的线程，以及是否需要睡眠或唤醒所有线程
//				state.constructDAG.queueMu.Lock()
//				if state.constructDAG.completedMergeCount == 0 { // 第一个完成构图的线程
//					for workerId, isWork := range state.constructDAG.constructThreads {
//						if isWork.Load() { // 构建了 DAG 的线程
//							state.constructDAG.completedThreads[workerId] = struct{}{}
//						}
//					}
//					fmt.Printf("[Worker %d] Batch %d: First thread completed DAG construction, total constructed threads: %d\n", workerID, state.BatchID, len(state.constructDAG.completedThreads))
//					state.constructDAG.initialCount = len(state.constructDAG.completedThreads)
//					state.constructDAG.totalMerges = state.constructDAG.initialCount*2 - 1
//					state.constructDAG.dagQueue = append(state.constructDAG.dagQueue, state.constructDAG.dags[workerID])
//					state.constructDAG.completedMergeCount++
//					completedMergeCount := state.constructDAG.completedMergeCount
//					totalMerges := state.constructDAG.totalMerges
//					state.constructDAG.queueMu.Unlock()
//
//					isWait := state.constructDAG.awakeOrWaitConstructDAG(state, completedMergeCount, totalMerges, workerID)
//					if isWait {
//						// 被唤醒之后进入下一阶段
//						pe.workerStaties[workerID].Phase = CommitMaximumValidationPhase
//					} // 完成之后，未睡眠，仍然在当前阶段
//
//				} else { // 不是第一个完成构图的线程，直接进行唤醒或等待
//					if _, ok := state.constructDAG.completedThreads[workerID]; !ok { // 之前没有记录过该线程
//						fmt.Printf("[Worker %d] Batch %d: Another thread completed DAG construction, total constructed threads before adding: %d\n", workerID, state.BatchID, len(state.constructDAG.completedThreads))
//						state.constructDAG.initialCount++
//						state.constructDAG.totalMerges = state.constructDAG.initialCount*2 - 1
//					}
//					state.constructDAG.completedMergeCount++
//					completedMergeCount := state.constructDAG.completedMergeCount
//					totalMerges := state.constructDAG.totalMerges
//					if completedMergeCount%2 == 1 || completedMergeCount == totalMerges { // 需要进入等待
//						state.constructDAG.dagQueue = append(state.constructDAG.dagQueue, state.constructDAG.dags[workerID])
//					} else {
//						// 偶数次合并，直接进行合并
//						pairDag = state.constructDAG.dagQueue[0]
//						state.constructDAG.dagQueue = state.constructDAG.dagQueue[1:]
//					}
//					state.constructDAG.queueMu.Unlock()
//
//					isWait := state.constructDAG.awakeOrWaitConstructDAG(state, completedMergeCount, totalMerges, workerID)
//					if isWait {
//						// 被唤醒之后进入下一阶段
//						pe.workerStaties[workerID].Phase = CommitMaximumValidationPhase
//					} // 完成之后，未睡眠，仍然在当前阶段
//				}
//			}
//
//			break // 队列已完成
//		}
//
//		// 抢一个 slot
//		if state.constructDAG.stateTableIndex.CompareAndSwap(int32(idx), int32(idx+2)) {
//			if state.constructDAG.dags[workerID] == nil {
//				state.constructDAG.constructThreads[workerID].Store(true)
//				state.constructDAG.dags[workerID] = &ConflictDAG{
//					Nodes:       make(map[int]*ReadWriteSet),
//					EdgeDetails: make(map[int][]*ConflictEdge),
//					InDegree:    make(map[int]int),
//				}
//			}
//			// 如果是偶数，idx可能越界，但会在上面的判断中break掉
//			// idx+1的下标处于最后一个或越界时，单独处理
//			if idx+1 >= len(state.constructDAG.stateTables)-1 {
//				for workerId, isWork := range state.constructDAG.constructThreads {
//					if isWork.Load() { // 构建了 DAG 的线程
//						state.constructDAG.completedThreads[workerId] = struct{}{}
//					}
//				}
//				fmt.Printf("[Worker %d] Batch %d: First thread completed DAG construction, total constructed threads: %d\n", workerID, state.BatchID, len(state.constructDAG.completedThreads))
//				state.constructDAG.initialCount = len(state.constructDAG.completedThreads)
//				state.constructDAG.totalMerges = state.constructDAG.initialCount*2 - 1
//				// 单数个 StateTable，最后一个单独处理
//				pe.constructDAGForAddress(state, state.constructDAG.stateTables[idx], nil, workerID)
//			} else {
//				// 正常配对处理
//				pe.constructDAGForAddress(state, state.constructDAG.stateTables[idx], state.constructDAG.stateTables[idx+1], workerID)
//			}
//			//break
//			// 这里无需break，继续循环尝试获取任务，直到抢完所有任务
//		}
//
//		// 抢失败，继续循环重试
//	}
//	return pairDag
//}

func (pe *PipelineEngine) tryConstructDAG(state *BatchState, workerID int) (pairDag *ConflictDAG) {
	// 循环尝试从队列中抢任务
	for {
		idx, ok := state.constructDAG.tryGetTaskAndActiveWorker(workerID)
		if !ok { // 队列已完成，等价于 idx >= len(state.constructDAG.stateTables)
			if state.constructDAG.dags[workerID] == nil { // 当前线程没有构建任何 DAG，说明没有任务可做，等待
				//fmt.Printf("[Worker %d] Batch %d: New join thread, but no more StateTables to construct DAG, waiting...\n", workerID, state.BatchID)
				//startWait := time.Now()
				//isWait := waitHere(workerID, &state.constructDAG.condMu, state.constructDAG.cond, &state.constructDAG.done)
				for !state.constructDAG.done.Load() {
					//isWait = true
				}
				//elapsed := time.Since(startWait)
				//fmt.Printf("[Woker %d] Batch %d: New join thread, waiting %t, entry new phase resumed after waiting %s\n", workerID, state.BatchID, isWait, elapsed)
				pe.workerStaties[workerID].Phase = CommitMaximumValidationPhase // 被唤醒之后进入下一阶段

			} else { // 当前线程已经构建了 DAG，需要判断是否为第一个完成的线程，以及是否需要睡眠或唤醒所有线程
				dag := state.constructDAG.dags[workerID]
				state.constructDAG.queueMu.Lock()
				state.constructDAG.completedMergeCount++
				completedMergeCount := state.constructDAG.completedMergeCount
				if completedMergeCount%2 == 1 || completedMergeCount == dag.totalMerges { // 需要进入等待
					state.constructDAG.dagQueue = append(state.constructDAG.dagQueue, dag)
				} else {
					// 偶数次合并，直接进行合并
					pairDag = state.constructDAG.dagQueue[0]
					state.constructDAG.dagQueue = state.constructDAG.dagQueue[1:]
				}
				state.constructDAG.queueMu.Unlock()

				isWait := state.constructDAG.awakeOrWaitConstructDAG(state, completedMergeCount, dag.totalMerges, workerID)
				if isWait {
					// 被唤醒之后进入下一阶段
					pe.workerStaties[workerID].Phase = CommitMaximumValidationPhase
				} // 完成之后，未睡眠，仍然在当前阶段
			}

			break // 队列已完成

		}

		// ok == true，抢一个 slot
		if state.constructDAG.dags[workerID] == nil {
			state.constructDAG.dags[workerID] = &ConflictDAG{
				Nodes: make(map[int]*ReadWriteSet),
				//EdgeDetails: make(map[int][]*ConflictEdge),
				Edges: make(map[int]map[int]struct{}),
				//InDegree:    make(map[int]int),
				Degree:      make(map[int]int),
				totalMerges: -1,
			}
		}
		// 如果是偶数，idx可能越界，但会在上面的判断中break掉
		// idx+1的下标处于最后一个或越界时，单独处理
		var rwTable2 *StateTable = nil
		if idx+1 >= len(state.constructDAG.stateTables)-1 {
			threadCount := state.constructDAG.CountActiveWorkersFast()
			state.constructDAG.dags[workerID].totalMerges = threadCount*2 - 1
			fmt.Printf("[Worker %d] Batch %d: Last thread completed DAG construction, total constructed threads: %d\n", workerID, state.BatchID, threadCount)
			if idx+1 < len(state.constructDAG.stateTables) { // 单数个 StateTable，最后一个单独处理
				rwTable2 = state.constructDAG.stateTables[idx+1]
			}
			pe.constructDAGForAddress(state, state.constructDAG.stateTables[idx], rwTable2, workerID)
		} else {
			// 正常配对处理
			pe.constructDAGForAddress(state, state.constructDAG.stateTables[idx], state.constructDAG.stateTables[idx+1], workerID)
		}
		//break
		// 这里无需break，继续循环尝试获取任务，直到抢完所有任务
	}
	return pairDag
}

// tryGetMergeTask 尝试获取合并任务
func (pe *PipelineEngine) tryMergeDag(state *BatchState, pairDag *ConflictDAG, workerID int) (newPairDag *ConflictDAG) {
	return pe.mergeTwoDags(state, pairDag, workerID)
}

// WaitForBlockCompletion 等待一个区块的所有批次完成
func (pe *PipelineEngine) WaitForBlockCompletion(batchCount int) []*ValidationResult {
	results := make([]*ValidationResult, 0, batchCount)

	for i := 0; i < batchCount; i++ {
		batchID := <-pe.completeChan

		// 从批次切片中获取结果
		if batchID < len(pe.batchStates) {
			state := pe.batchStates[batchID]

			state.mu.Lock()
			result := &ValidationResult{
				CommittedTxs: state.CommittedTxs,
				AbortedTxs:   state.AbortedTxs,
			}
			state.mu.Unlock()

			results = append(results, result)

			fmt.Printf("[Completed] Batch %d: %d committed, %d aborted\n",
				batchID, len(result.CommittedTxs), len(result.AbortedTxs))
		}
	}

	return results
}

// completeBatch 完成批次
func (pe *PipelineEngine) completeBatch(state *BatchState) {
	state.ValidationDone.Store(1)
	pe.completeChan <- state.BatchID
	//pe.needSwitch.Store(1)

	fmt.Printf("[Batch %d] Marked as complete, notifying switch\n", state.BatchID)
}
