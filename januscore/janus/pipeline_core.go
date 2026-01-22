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
			CompletionOrder:        make([]int, 0),
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
		//fmt.Printf("Worker %d is running, stateId=%d, worker stateId=%d\n", workerID, stateID, pe.workerStaties[workerID].currentBatchID)
		// 切换到新的批次状态
		if stateID > pe.workerStaties[workerID].currentBatchID {
			pe.workerStaties[workerID].currentBatchID = stateID
			pe.workerStaties[workerID].Phase = ExecuteCurrentBatchPhase
			if enableLog {
				fmt.Printf("[Worker %d] Switched to Batch %d\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
		}

		state := pe.batchStates[stateID]
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

		if pe.workerStaties[workerID].Phase == ConstructDAGPhase {
			if enableLog {
				fmt.Printf("[Worker %d] [batch %d] entried construct dag phase and busy waitting.\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
			for !state.MergeThreadStateTables.done.Load() {
				// busy wait
				//fmt.Printf("[Worker %d] Skipping task execution phase, merge=%t\n", workerID, state.MergeThreadStateTables.done.Load())
			}
			if pe.changeToNextBatch(state, workerID) { // 直接切换到下一批
				continue
			}
			if enableLog {
				fmt.Printf("[Worker %d] [batch %d] entried construct dag phase and try to construct DAG.\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
			// todo:尝试获取合并任务, 寻找弱连通分量
			pairDag := pe.tryConstructDAG(state, workerID)
			for pairDag != nil {
				pairDag = pe.tryMergeDag(state, pairDag, workerID)
			}
			if enableLog {
				fmt.Printf("[Worker %d] [batch %d] finished construct dag phase and try to entry commit maximum validation phase.\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
		}

		if pe.workerStaties[workerID].Phase == CommitMaximumValidationPhase {
			for !state.constructDAG.done.Load() {
				// busy wait
			}
			if enableLog {
				fmt.Printf("[Worker %d] [batch %d] entried commit maximum validation phase.\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
			if pe.changeToNextBatch(state, workerID) { // 直接切换到下一批
				continue
			}
			// 并发求解最大权重独立集
			pe.solveComponentMWIS(state, workerID)
			if enableLog {
				fmt.Printf("[Worker %d] [batch %d] finished commit maximum validation phase and try to entry re-execute phase.\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
		}

		// ========== 重执行阶段一（并发重执行） ==========
		if pe.workerStaties[workerID].Phase == ReExecutePhase {
			// 等待 MWIS 阶段完成
			for !state.constructDAG.mwisDone.Load() {
				// busy wait
			}
			if enableLog {
				fmt.Printf("[Worker %d] [batch %d] entried commit re-execute parallel phase.\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
			if pe.changeToNextBatch(state, workerID) { // 直接切换到下一批
				continue
			}
			pe.reExecute(state, workerID)
			if enableLog {
				fmt.Printf("[Worker %d] [batch %d] finished re-execute parallel phase and try to entry re-execute serial phase.\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
		}
	}
}

// solveComponentMWIS 并发求解连通分量的最大权重独立集
func (pe *PipelineEngine) solveComponentMWIS(state *BatchState, workerID int) {
	cdr := state.constructDAG

	// 获取最终的 DAG（用于求解）
	finalDag := cdr.dagQueue[0]

	if cdr.totalComponents == 0 { // TODO：全部都可以提交，直接进入下一批
		pe.tryEntryNextBatch(state, workerID)
		return
	}

	// 循环抢任务求解
	for {
		// 尝试获取一个连通分量
		idx := int(cdr.componentsIndex.Add(1) - 1)
		if idx >= cdr.totalComponents {
			// 没有更多任务，等待所有求解完成
			break
		}

		// 获取该连通分量的节点
		nodes := cdr.componentsQueue[idx]

		// 求解该连通分量的最大权重独立集
		var independentSet, abortSet []int
		var err error

		// 单节点情况已经处理过了，直接调用 ILP 求解多节点的情况
		//if enableLog {
		//	fmt.Printf("[Worker %d] [batch %d] [MWIS] connected component index=%d, nodes=%v，solving MWIS.\n",
		//		workerID, state.BatchID, idx, nodes)
		//}

		independentSet, err = SolveMWIS(finalDag, nodes)
		if err != nil {
			fmt.Printf("[Error] [Worker %d] [batch %d] [MWIS] connected component index=%d err: %v\n",
				workerID, state.BatchID, idx, err)
			independentSet = []int{} // 求解失败，保守策略：不提交任何交易
		} else {
			independentMap := make(map[int]struct{})
			for _, nodeID := range independentSet {
				independentMap[nodeID] = struct{}{}
			}
			for _, node := range nodes {
				if _, exist := independentMap[node]; !exist { // 不在独立集中，那么则丢弃
					abortSet = append(abortSet, node)
				}
			}
			//if enableLog {
			//	// 计算总权重
			//	totalWeight := 0.0
			//	for _, nodeID := range independentSet {
			//		if rwset, exists := finalDag.Nodes[nodeID]; exists && rwset != nil {
			//			totalWeight += rwset.Cost
			//		}
			//	}
			//	abortWeight := 0.0
			//	for _, nodeID := range abortSet {
			//		if rwset, exists := finalDag.Nodes[nodeID]; exists && rwset != nil {
			//			abortWeight += rwset.Cost
			//		}
			//	}
			//
			//	fmt.Printf("[Worker %d] [batch %d] [MWIS] connected component index=%d, nodes=%v, independent set=%v with weight=%.2f, abort set=%v with weight=%.2f\n",
			//		workerID, state.BatchID, idx, nodes, independentSet, totalWeight, abortSet, abortWeight)
			//}
		}

		// 将结果加入提交集合
		if cdr.threadCommittedTxs[workerID] == nil {
			cdr.threadCommittedTxs[workerID] = make([]int, 0)
		}
		cdr.threadCommittedTxs[workerID] = append(cdr.threadCommittedTxs[workerID], independentSet...)

		if cdr.threadAbortedTxs[workerID] == nil {
			cdr.threadAbortedTxs[workerID] = make([][]int, 0)
		}
		cdr.threadAbortedTxs[workerID] = append(cdr.threadAbortedTxs[workerID], abortSet)

		// 检查是否所有连通分量都已求解
		if idx == cdr.totalComponents-1 {
			// 最后一个完成的线程负责汇总结果
			pe.finalizeMWISResults(state, workerID)
			break
		}
	}

	// 进入下一阶段
	pe.workerStaties[workerID].Phase = ReExecutePhase
}

// finalizeMWISResults 汇总 MWIS 求解结果
func (pe *PipelineEngine) finalizeMWISResults(state *BatchState, workerID int) {
	cdr := state.constructDAG
	finalDag := cdr.dagQueue[0]

	// ========== 性能测试 ==========
	if EnableMWISBenchmark {
		BenchmarkMWIS(finalDag)
	}

	// 合并各线程提交的交易
	for _, commitTxs := range cdr.threadCommittedTxs {
		if commitTxs != nil {
			state.CommittedTxs = append(state.CommittedTxs, commitTxs...)
		}
	}

	// 合并各线程丢弃的交易，以连通分量的形式
	for _, threadAbortedTxs := range cdr.threadAbortedTxs {
		if threadAbortedTxs != nil {
			cdr.abortedTxs = append(cdr.abortedTxs, threadAbortedTxs...)
		}
	}

	// 收集提交和中止的交易
	//committedTxs := make([]*ReadWriteSet, 0)
	//abortedTxs := make([]*ReadWriteSet, 0)

	//for nodeID, rwset := range finalDag.Nodes {
	//	if _, exist := cdr.committedTxs[nodeID]; exist {
	//		committedTxs = append(committedTxs, rwset)
	//	} else {
	//		abortedTxs = append(abortedTxs, rwset)
	//	}
	//}

	//// 更新批次状态
	//state.CommittedTxs = committedTxs
	//state.AbortedTxs = abortedTxs

	// 打印最终结果
	//fmt.Printf("\n========== MWIS 求解结果 (Batch %d) ==========\n", state.BatchID)
	//fmt.Printf("总交易数: %d\n", len(finalDag.Nodes))
	//fmt.Printf("提交交易数: %d\n", len(committedTxs))
	//fmt.Printf("中止交易数: %d\n", len(abortedTxs))

	// 打印提交的交易ID
	//commitIDs := make([]int, 0, len(committedTxs))
	//for _, rwset := range committedTxs {
	//	commitIDs = append(commitIDs, rwset.TxID)
	//}
	//fmt.Printf("提交交易: %v\n", commitIDs)

	// 打印中止的交易ID
	//abortIDs := make([]int, 0, len(abortedTxs))
	//for _, rwset := range abortedTxs {
	//	abortIDs = append(abortIDs, rwset.TxID)
	//}
	//fmt.Printf("中止交易: %v\n", abortIDs)
	//fmt.Printf("==============================================\n\n")
	// ========== 判断是否需要重执行 ==========
	if len(cdr.abortedTxs) > 0 {
		// 初始化重执行状态
		state.reExecute = newReExecuteState(finalDag, cdr.abortedTxs, pe.numThreads)
	}
	cdr.mwisDone.Store(true) // 标记 MWIS 阶段完成
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
			//fmt.Printf("test write set\n")
			//fmt.Println(preState.writeSet)
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
	//fmt.Printf("[Janus] executeCurrentBatch workerID=%d, BatchID=%d, WokerBatchID=%d\n", workerID, state.BatchID, pe.workerStaties[workerID].currentBatchID)
	//fmt.Println("&state.LongTxIndex", &state.LongTxIndex)
	//fmt.Println("workerID", workerID)
	//fmt.Println("state batch =", state.BatchID, state)
	//fmt.Println("&state.LongTxs", &state.LongTxs)
	pe.executeNextTransaction(&state.LongTxIndex, &state.LongTxs, workerID, state)

	// ========= 2. 执行短交易 =========
	pe.executeNextTransaction(&state.ShortTxIndex, &state.ShortTxs, workerID, state)

	// ========= 3. 当前批次没有交易可以执行, 尝试部分线程进行下一批的执行 =========
	// 线程完成本批次的任务后，需要先构建自己的状态读写表
	if state.ThreadStateTables[workerID] == nil {
		state.ThreadStateTables[workerID] = pe.buildStateTable(state, workerID) // 如果未参与构建, 则可能为空
		if enableLog {
			if state.ThreadStateTables[workerID] == nil {
				fmt.Printf("[WARNNING] [Worker %d] not execute any transaction\n", workerID)
			}
		}
	}

	// 说明：这里设计成部分线程进入下一批执行，是为了避免所有线程都进入下一批，导致当前批次无法完成，从而阻塞流水线
	state.CompletionMu.Lock()
	threadNextNumber := len(state.CompletionOrder) // 有多少线程已经完成当前批次并并已经进入下一批
	if threadNextNumber >= (pe.numThreads+1)/2 {   // 已经有超过一半的线程去执行下一批, 剩下的进行合并state table
		pairIndex := state.pairIndex // 取一个配对的 threadStateTable
		state.pairIndex++            // 放行
		state.CompletionMu.Unlock()

		if pairIndex == 0 && pe.numThreads%2 == 1 { // 奇数线程时，最后一个线程无需配对
			lastWorkerID := state.CompletionOrder[threadNextNumber-1]
			state.MergeThreadStateTables.queueMu.Lock()
			state.MergeThreadStateTables.stateTablesQueue = append(state.MergeThreadStateTables.stateTablesQueue, state.ThreadStateTables[lastWorkerID])
			state.MergeThreadStateTables.queueMu.Unlock()
		}
		if enableLog {
			fmt.Printf("[Worker %d] [batch %d] execute current batch, pairIndex=%d, "+
				"len(state.CompletionOrder)=%d, threadNextNumber=%d, (pe.numThreads+1)/2=%d\n",
				workerID, state.BatchID, pairIndex, len(state.CompletionOrder), threadNextNumber, (pe.numThreads+1)/2)
		}
		pairWorkerID := state.CompletionOrder[pairIndex]
		return state.ThreadStateTables[pairWorkerID], true // 返回配对的线程ID
	}

	// 部分线程可以切换到下一批
	state.CompletionOrder = append(state.CompletionOrder, workerID) // 先加入完成队列，防止竞争
	state.CompletionMu.Unlock()
	if state.NextBatch != nil {
		pe.workerStaties[workerID].Phase = PreExecuteNextBatchPhase // 成功增加线程数
	} else { // 如果没有下一批, 即最后一批，那么直接进入本批次的下一阶段
		pe.workerStaties[workerID].Phase = ConstructDAGPhase
	}
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

func (pe *PipelineEngine) tryConstructDAG(state *BatchState, workerID int) (pairDag *ConflictDAG) {
	// 循环尝试从队列中抢任务
	for {
		idx, ok := state.constructDAG.tryGetTaskAndActiveWorker(workerID)
		if !ok { // 队列已完成，等价于 idx >= len(state.constructDAG.stateTables)
			if state.constructDAG.dags[workerID] == nil { // 当前线程没有构建任何 DAG，说明没有任务可做，等待
				if enableLog {
					fmt.Printf("[WARNNING] [Worker %d] [batch %d] not found dag task\n", workerID, state.BatchID)
				}
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
			break
		}

		// ok == true，抢一个 slot
		if state.constructDAG.dags[workerID] == nil {
			state.constructDAG.dags[workerID] = &ConflictDAG{
				Nodes:       make(map[int]*ReadWriteSet),
				Edges:       make(map[int]map[int]struct{}),
				Degree:      make(map[int]int),
				totalMerges: -1,
				parent:      make(map[int]int),
				rank:        make(map[int]int),
			}
		}
		// idx可能越界，但会在上面的判断中break掉
		// idx+1 处于最后一个或越界时，单独处理
		var rwTable2 *StateTable = nil
		if idx+1 >= len(state.constructDAG.stateTables)-1 {
			threadCount := state.constructDAG.CountActiveWorkersFast()
			state.constructDAG.dags[workerID].totalMerges = threadCount*2 - 1
			if enableLog {
				fmt.Printf("[Worker %d] [batch %d]: Last thread completed DAG construction, total constructed threads: %d\n", workerID, state.BatchID, threadCount)
			}
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
func (pe *PipelineEngine) WaitForBlockCompletion(batchCount int) {

	for i := 0; i < batchCount; i++ {
		<-pe.completeChan
		//batchID := <-pe.completeChan
		//
		//// 从批次切片中获取结果
		//if batchID < len(pe.batchStates) {
		//	state := pe.batchStates[batchID]
		//	fmt.Printf("[Completed] Batch %d: %d committed, %d aborted\n",
		//		batchID, len(result.CommittedTxs), len(result.AbortedTxs))
		//}
	}
}

// completeBatch 完成批次
func (pe *PipelineEngine) completeBatch(state *BatchState) {
	pe.completeChan <- state.BatchID
}

// ExecuteParallelPhase 重执行阶段一：并发重执行
// 根据原始冲突图的依赖关系，多线程并发重执行被丢弃的交易
// 重执行后比较读写集，读写集未变的可以提交，读写集变化的进入阶段二串行执行
func (pe *PipelineEngine) reExecute(state *BatchState, workerID int) {
	reExec := state.reExecute
	if reExec == nil {
		pe.tryEntryNextBatch(state, workerID) // 没有需要重执行的交易，进入下一批
		return
	}

	// 循环抢任务重执行
	for {
		// 尝试获取一个待重执行的交易
		idx := int(reExec.abortedTxComponentsIndex.Add(1) - 1)
		if idx >= reExec.totalAbortedTxComponents {
			// 没有更多任务
			if !reExec.done.Load() {
				// busy wait
			}
			pe.tryEntryNextBatch(state, workerID) // 最后一个阶段完成了，进入下一批
			break
		}

		txIds := reExec.abortedTxComponents[idx]
		sort.Ints(txIds)

		// 获取原始读写集
		for _, txId := range txIds {
			oldRWSet := reExec.cg.Nodes[txId]
			newRWSet := pe.reExecuteTransaction(oldRWSet, workerID) // 重执行交易，获取新的读写集
			// 比较读写集是否变化
			rwSetChanged := !pe.compareRWSet(oldRWSet, newRWSet)
			if rwSetChanged {
				if enableLog {
					fmt.Printf("[Worker %d] [batch %d] [ReExecute Phase1] 交易 %d 读写集变化，进入阶段二\n", workerID, state.BatchID, oldRWSet.TxID)
				}
				if reExec.threadAbortedTxs[workerID] == nil {
					reExec.threadAbortedTxs[workerID] = make([]int, 0)
				}
				reExec.threadAbortedTxs[workerID] = append(reExec.threadAbortedTxs[workerID], txId)
			} else {
				// 读写集未变，可以提交
				//if enableLog {
				//	fmt.Printf("[Worker %d] [ReExecute Phase1] 交易 %d 读写集未变，提交成功\n", workerID, oldRWSet.TxID)
				//}
				if reExec.threadCommittedTxs[workerID] == nil {
					reExec.threadCommittedTxs[workerID] = make([]int, 0)
				}
				reExec.threadCommittedTxs[workerID] = append(reExec.threadCommittedTxs[workerID], txId)
			}
		}

		// 检查是否所有的连通分量都处理完成
		if idx == reExec.totalAbortedTxComponents-1 { // 最后一个完成的线程负责串行执行剩下的交易
			pe.finalizeReExecute(state, workerID)
			break
		}
	}
	pe.tryEntryNextBatch(state, workerID) // 最后一个阶段完成了，进入下一批
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

// finalizeReExecute 串行执行被丢弃的交易
func (pe *PipelineEngine) finalizeReExecute(state *BatchState, workerID int) {
	reExec := state.reExecute

	// 合并各线程提交的交易
	for _, commitTxs := range reExec.threadCommittedTxs {
		if commitTxs != nil {
			state.CommittedTxs = append(state.CommittedTxs, commitTxs...)
		}
	}

	// 合并各线程丢弃的交易，以连通分量的形式
	for _, threadAbortedTxs := range reExec.threadAbortedTxs {
		if threadAbortedTxs != nil {
			reExec.abortedTxs = append(reExec.abortedTxs, threadAbortedTxs...)
		}
	}
	sort.Ints(reExec.abortedTxs)

	// 打印阶段一结果
	if enableLog {
		fmt.Printf("[Worker %d] [batch %d] 交易%v需要串行执行.\n", workerID, state.BatchID, reExec.abortedTxs)
	}

	for _, txId := range reExec.abortedTxs {
		pe.reExecuteTransaction(reExec.cg.Nodes[txId], workerID) // 重执行交易
	}

	// 标记阶段一完成
	reExec.done.Store(true)
}
