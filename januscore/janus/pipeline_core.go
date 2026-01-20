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
			writeSet:               make(map[string]struct{}),
			ThreadRWSets:           make([][]*ReadWriteSet, pe.numThreads),
			ThreadStateTables:      make([]*stateTableWithWriteSet, pe.numThreads),
			MergeThreadStateTables: newThreadStateTableForMerge(pe.numThreads),
			constructDAG:           newConstructDAGResult(pe.numThreads),
			reExecute:              nil,
			CompletionOrder:        make([]int, 0),
			TotalTxs:               len(batch.AllTxs),
			threadNumber:           int32(pe.numThreads),
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
		if pe.currentBatchID.Load() >= int32(len(pe.batchStates)) {
			return // 完成所有批次
		}

		if pe.currentBatchID.Load() == -1 { // 尚未有批次
			continue
		}
		// 切换到新的批次状态
		if pe.currentBatchID.Load() > pe.workerStaties[workerID].currentBatchID {
			pe.workerStaties[workerID].currentBatchID = pe.currentBatchID.Load()
			pe.workerStaties[workerID].Phase = ExecuteCurrentBatchPhase
			if enableLog {
				fmt.Printf("[Worker %d] Switched to Batch %d\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
		}

		state := pe.batchStates[pe.currentBatchID.Load()]
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
			// 并发求解最大权重独立集
			pe.solveComponentMWIS(state, workerID)
			if enableLog {
				fmt.Printf("[Worker %d] [batch %d] finished commit maximum validation phase and try to entry re-execute phase.\n", workerID, pe.workerStaties[workerID].currentBatchID)
			}
		}

		// ========== 重执行阶段一（并发重执行） ==========
		if pe.workerStaties[workerID].Phase == ReExecutePhase {
			pe.reExecutePhase1(state, workerID)
			continue
		}

		// ========== 重执行阶段二（串行执行） ==========
		if pe.workerStaties[workerID].Phase == SerialExecutePhase {
			pe.reExecutePhase2(state, workerID)
			continue
		}
	}
}

// solveComponentMWIS 并发求解连通分量的最大权重独立集
func (pe *PipelineEngine) solveComponentMWIS(state *BatchState, workerID int) {
	cdr := state.constructDAG

	// 获取最终的 DAG（用于求解）
	finalDag := cdr.dagQueue[0]

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
		var independentSet []int
		var err error

		if len(nodes) == 1 {
			// 单节点直接提交
			independentSet = nodes
			fmt.Printf("[Worker %d] [MWIS] 连通分量 %d: 单节点 %v，直接提交\n",
				workerID, idx, nodes)
		} else {
			// 多节点，调用 ILP 求解
			fmt.Printf("[Worker %d] [MWIS] 连通分量 %d: 节点=%v，开始求解...\n",
				workerID, idx, nodes)

			independentSet, err = SolveMWIS(finalDag, nodes)
			if err != nil {
				fmt.Printf("[Worker %d] [MWIS] 连通分量 %d 求解失败: %v\n",
					workerID, idx, err)
				// 求解失败，保守策略：不提交任何交易
				independentSet = []int{}
			} else {
				// 计算总权重
				totalWeight := 0.0
				for _, nodeID := range independentSet {
					if rwset, exists := finalDag.Nodes[nodeID]; exists && rwset != nil {
						totalWeight += rwset.Cost
					}
				}
				fmt.Printf("[Worker %d] [MWIS] 连通分量 %d: 独立集=%v, 总权重=%.2f\n",
					workerID, idx, independentSet, totalWeight)
			}
		}

		// 将结果加入提交集合
		if cdr.commitTxs[workerID] == nil {
			cdr.commitTxs[workerID] = make([]int, 0)
		}
		for _, txID := range independentSet {
			cdr.commitTxs[workerID] = append(cdr.commitTxs[workerID], txID)
		}

		// 增加已完成计数
		solved := cdr.solvedCount.Add(1)

		// 检查是否所有连通分量都已求解
		if int(solved) == cdr.totalComponents {
			// 最后一个完成的线程负责汇总结果
			pe.finalizeMWISResults(state, workerID)
		}
	}

	// 等待 MWIS 阶段完成
	for !cdr.mwisDone.Load() {
		// busy wait
	}

	// 进入下一阶段或完成批次
	// ========== 根据是否有重执行决定下一阶段 ==========
	if state.reExecute != nil && state.reExecute.phase1Total > 0 {
		// 有需要重执行的交易，进入重执行阶段
		pe.workerStaties[workerID].Phase = ReExecutePhase
	} else {
		// 没有需要重执行的交易，进入等待阶段
		pe.reEntryWaitingTaskPhase(state, workerID)
	}
}

// finalizeMWISResults 汇总 MWIS 求解结果
func (pe *PipelineEngine) finalizeMWISResults(state *BatchState, workerID int) {
	cdr := state.constructDAG
	finalDag := cdr.dagQueue[0]

	// ========== 性能测试 ==========
	if EnableMWISBenchmark {
		BenchmarkMWIS(finalDag)
	}

	// 收集提交和中止的交易
	committedTxs := make([]*ReadWriteSet, 0)
	abortedTxs := make([]*ReadWriteSet, 0)

	for _, commitTxs := range cdr.commitTxs {
		if commitTxs != nil {
			for _, commitTx := range commitTxs {
				cdr.committedTxs[commitTx] = struct{}{}
			}
		}
	}

	for nodeID, rwset := range finalDag.Nodes {
		if _, exist := cdr.committedTxs[nodeID]; exist {
			committedTxs = append(committedTxs, rwset)
		} else {
			abortedTxs = append(abortedTxs, rwset)
		}
	}

	// 更新批次状态
	state.mu.Lock()
	state.CommittedTxs = committedTxs
	state.AbortedTxs = abortedTxs
	state.mu.Unlock()

	// 打印最终结果
	fmt.Printf("\n========== MWIS 求解结果 (Batch %d) ==========\n", state.BatchID)
	fmt.Printf("总交易数: %d\n", len(finalDag.Nodes))
	fmt.Printf("提交交易数: %d\n", len(committedTxs))
	fmt.Printf("中止交易数: %d\n", len(abortedTxs))

	// 打印提交的交易ID
	commitIDs := make([]int, 0, len(committedTxs))
	for _, rwset := range committedTxs {
		commitIDs = append(commitIDs, rwset.TxID)
	}
	fmt.Printf("提交交易: %v\n", commitIDs)

	// 打印中止的交易ID
	abortIDs := make([]int, 0, len(abortedTxs))
	for _, rwset := range abortedTxs {
		abortIDs = append(abortIDs, rwset.TxID)
	}
	fmt.Printf("中止交易: %v\n", abortIDs)
	fmt.Printf("==============================================\n\n")
	// ========== 判断是否需要重执行 ==========
	if len(abortedTxs) > 0 {
		// 初始化重执行状态
		state.reExecute = newReExecuteState()

		// 按原始交易顺序排序中止的交易
		sortedAborted := make([]*ReadWriteSet, len(abortedTxs))
		copy(sortedAborted, abortedTxs)
		sortByTxID(sortedAborted)

		state.reExecute.phase1Queue = sortedAborted
		state.reExecute.phase1Total = len(sortedAborted)

		// 简单日志
		// fmt.Printf("\n[ReExecute] 初始化重执行，待重执行交易数: %d\n", len(sortedAborted))
		// fmt.Printf("[ReExecute] 待重执行交易ID: ")
		// for _, rwset := range sortedAborted {
		//     fmt.Printf("%d ", rwset.TxID)
		// }
		// fmt.Println()

		// 详细日志
		pe.printAbortedTxDependencyGraph(finalDag, sortedAborted, state.BatchID)

		// 标记 MWIS 阶段完成
		cdr.mwisDone.Store(true)
	} else {
		// 没有需要重执行的交易，直接完成批次
		cdr.mwisDone.Store(true)
	}
}

// printAbortedTxDependencyGraph 打印丢弃交易构成的依赖图
func (pe *PipelineEngine) printAbortedTxDependencyGraph(dag *ConflictDAG, abortedTxs []*ReadWriteSet, batchID int) {
	fmt.Printf("\n╔══════════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║         丢弃交易依赖图 (Batch %d)                                 ║\n", batchID)
	fmt.Printf("╠══════════════════════════════════════════════════════════════════╣\n")

	// 构建丢弃交易的ID集合
	abortedSet := make(map[int]bool)
	for _, rwset := range abortedTxs {
		abortedSet[rwset.TxID] = true
	}

	// 提取丢弃交易之间的边
	abortedEdges := make([][2]int, 0)
	edgeSet := make(map[string]bool)

	for _, rwset := range abortedTxs {
		txID := rwset.TxID
		if dag.Edges[txID] == nil {
			continue
		}
		for neighborID := range dag.Edges[txID] {
			// 只保留丢弃交易之间的边
			if !abortedSet[neighborID] {
				continue
			}
			// 边去重：只保留 (小, 大) 形式
			u, v := txID, neighborID
			if u > v {
				u, v = v, u
			}
			edgeKey := fmt.Sprintf("%d-%d", u, v)
			if !edgeSet[edgeKey] {
				edgeSet[edgeKey] = true
				abortedEdges = append(abortedEdges, [2]int{u, v})
			}
		}
	}

	// 打印基本信息
	fmt.Printf("║ 丢弃交易数: %-53d ║\n", len(abortedTxs))
	fmt.Printf("║ 依赖边数:   %-53d ║\n", len(abortedEdges))
	fmt.Printf("╠══════════════════════════════════════════════════════════════════╣\n")

	// 打印丢弃交易的ID和权重
	fmt.Printf("║ 丢弃交易详情:                                                    ║\n")
	for _, rwset := range abortedTxs {
		fmt.Printf("║   tx%-3d: 权重=%-8.2f 读集=%v 写集=%v\n",
			rwset.TxID, rwset.Cost, getKeys(rwset.ReadSet), getKeys(rwset.WriteSet))
	}
	fmt.Printf("╠══════════════════════════════════════════════════════════════════╣\n")

	// 打印依赖边
	fmt.Printf("║ 依赖边（冲突关系）:                                              ║\n")
	if len(abortedEdges) == 0 {
		fmt.Printf("║   （无依赖边，所有丢弃交易相互独立）                             ║\n")
	} else {
		for _, edge := range abortedEdges {
			u, v := edge[0], edge[1]
			// 找出冲突的地址
			conflictAddrs := pe.findConflictAddresses(dag.Nodes[u], dag.Nodes[v])
			fmt.Printf("║   tx%-3d ←→ tx%-3d  冲突地址: %v\n", u, v, conflictAddrs)
		}
	}
	fmt.Printf("╠══════════════════════════════════════════════════════════════════╣\n")

	// 构建丢弃交易的子图并查找连通分量
	abortedComponents := pe.findAbortedComponents(abortedTxs, dag)
	fmt.Printf("║ 丢弃交易连通分量数: %-45d ║\n", len(abortedComponents))
	for i, component := range abortedComponents {
		fmt.Printf("║   分量%d: %v (大小=%d)\n", i+1, component, len(component))
	}
	fmt.Printf("╠══════════════════════════════════════════════════════════════════╣\n")

	// 打印每个丢弃交易的邻接关系
	fmt.Printf("║ 邻接表（仅丢弃交易内部）:                                        ║\n")
	for _, rwset := range abortedTxs {
		txID := rwset.TxID
		neighbors := make([]int, 0)
		if dag.Edges[txID] != nil {
			for neighborID := range dag.Edges[txID] {
				if abortedSet[neighborID] {
					neighbors = append(neighbors, neighborID)
				}
			}
		}
		if len(neighbors) > 0 {
			fmt.Printf("║   tx%-3d → %v\n", txID, neighbors)
		} else {
			fmt.Printf("║   tx%-3d → （无邻接丢弃交易）\n", txID)
		}
	}

	fmt.Printf("╚══════════════════════════════════════════════════════════════════╝\n\n")
}

// findConflictAddresses 找出两个交易之间冲突的地址
func (pe *PipelineEngine) findConflictAddresses(rw1, rw2 *ReadWriteSet) []string {
	if rw1 == nil || rw2 == nil {
		return []string{}
	}

	conflicts := make([]string, 0)
	seen := make(map[string]bool)

	// Write-Write 冲突
	for addr := range rw1.WriteSet {
		if _, exists := rw2.WriteSet[addr]; exists {
			if !seen[addr] {
				conflicts = append(conflicts, addr)
				seen[addr] = true
			}
		}
	}

	// Read-Write 冲突
	for addr := range rw1.ReadSet {
		if _, exists := rw2.WriteSet[addr]; exists {
			if !seen[addr] {
				conflicts = append(conflicts, addr)
				seen[addr] = true
			}
		}
	}

	// Write-Read 冲突
	for addr := range rw1.WriteSet {
		if _, exists := rw2.ReadSet[addr]; exists {
			if !seen[addr] {
				conflicts = append(conflicts, addr)
				seen[addr] = true
			}
		}
	}

	return conflicts
}

// findAbortedComponents 找出丢弃交易的连通分量
func (pe *PipelineEngine) findAbortedComponents(abortedTxs []*ReadWriteSet, dag *ConflictDAG) [][]int {
	// 构建丢弃交易的ID集合
	abortedSet := make(map[int]bool)
	for _, rwset := range abortedTxs {
		abortedSet[rwset.TxID] = true
	}

	// 使用并查集找连通分量
	parent := make(map[int]int)
	rank := make(map[int]int)

	var find func(x int) int
	find = func(x int) int {
		if _, exists := parent[x]; !exists {
			parent[x] = x
			rank[x] = 0
		}
		if parent[x] != x {
			parent[x] = find(parent[x])
		}
		return parent[x]
	}

	union := func(x, y int) {
		rootX := find(x)
		rootY := find(y)
		if rootX != rootY {
			if rank[rootX] < rank[rootY] {
				parent[rootX] = rootY
			} else if rank[rootX] > rank[rootY] {
				parent[rootY] = rootX
			} else {
				parent[rootY] = rootX
				rank[rootX]++
			}
		}
	}

	// 初始化所有丢弃交易
	for _, rwset := range abortedTxs {
		find(rwset.TxID)
	}

	// 根据边合并
	for _, rwset := range abortedTxs {
		txID := rwset.TxID
		if dag.Edges[txID] == nil {
			continue
		}
		for neighborID := range dag.Edges[txID] {
			if abortedSet[neighborID] {
				union(txID, neighborID)
			}
		}
	}

	// 收集连通分量
	components := make(map[int][]int)
	for _, rwset := range abortedTxs {
		root := find(rwset.TxID)
		components[root] = append(components[root], rwset.TxID)
	}

	// 转换为切片
	result := make([][]int, 0, len(components))
	for _, nodes := range components {
		// 排序
		sortIntSlice(nodes)
		result = append(result, nodes)
	}

	return result
}

// getKeys 获取 map 的键列表
func getKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// sortIntSlice 对整数切片排序
func sortIntSlice(s []int) {
	n := len(s)
	for i := 0; i < n-1; i++ {
		for j := i + 1; j < n; j++ {
			if s[i] > s[j] {
				s[i], s[j] = s[j], s[i]
			}
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

		if enableLog { // 打印日志信息
			if jtx.IsRuned && needRun {
				fmt.Printf("[Worker %d] [batch %d] tx %d already runed, but have conflict with pre batch, need re-execute.\n", workerID, state.BatchID, jtx.OriginalIdx)
			} else if !jtx.IsRuned {
				fmt.Printf("[Worker %d] [batch %d] tx %d need run.\n", workerID, state.BatchID, jtx.OriginalIdx)
			} else if !needRun {
				fmt.Printf("[Worker %d] [batch %d] tx %d already runed, and not need run.\n", workerID, state.BatchID, jtx.OriginalIdx)
			}
		}

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
			if pe.currentBatchID.Load() != int32(state.BatchID) { // 如果已经切换批次，则停止执行
				return true
			}

			idx := int(atomicIdx.Add(1) - 1) // 下标从0开始的
			if idx >= len(*txs) {            // 没有交易可以执行
				return false
			}

			pe.executeTransaction((*txs)[idx], workerID)
			(*txs)[idx].IsRuned = true // 必须标注

			if pe.currentBatchID.Load() != int32(state.BatchID) { // 如果已经切换批次，则停止执行
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
		if enableLog {
			fmt.Printf("[WARNNING] [Worker %d] pipeline engine was entried next batch(%d), "+
				"this worker is entring waiting phase.\n", workerID, pe.currentBatchID.Load())
		}
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

// reExecutePhase1 重执行阶段一：并发重执行
// 根据原始冲突图的依赖关系，多线程并发重执行被丢弃的交易
// 重执行后比较读写集，读写集未变的可以提交，读写集变化的进入阶段二串行执行
func (pe *PipelineEngine) reExecutePhase1(state *BatchState, workerID int) {
	reExec := state.reExecute
	if reExec == nil {
		// 没有需要重执行的交易，进入等待
		pe.reEntryWaitingTaskPhase(state, workerID)
		return
	}

	// 循环抢任务重执行
	for {
		// 尝试获取一个待重执行的交易
		idx := int(reExec.phase1Index.Add(1) - 1)
		if idx >= reExec.phase1Total {
			// 没有更多任务
			break
		}

		// 获取原始读写集
		oldRWSet := reExec.phase1Queue[idx]

		fmt.Printf("[Worker %d] [ReExecute Phase1] 重执行交易 %d\n", workerID, oldRWSet.TxID)

		// 重执行交易，获取新的读写集
		newRWSet := pe.reExecuteTransaction(oldRWSet, workerID)

		// 比较读写集是否变化
		rwSetChanged := !pe.compareRWSet(oldRWSet, newRWSet)

		reExec.phase1Mu.Lock()
		if rwSetChanged {
			// 读写集变化，加入阶段二队列
			fmt.Printf("[Worker %d] [ReExecute Phase1] 交易 %d 读写集变化，进入阶段二\n", workerID, oldRWSet.TxID)
			reExec.phase1Aborted = append(reExec.phase1Aborted, newRWSet)
		} else {
			// 读写集未变，可以提交
			fmt.Printf("[Worker %d] [ReExecute Phase1] 交易 %d 读写集未变，提交成功\n", workerID, oldRWSet.TxID)
			reExec.phase1Committed = append(reExec.phase1Committed, newRWSet)
		}
		reExec.phase1Mu.Unlock()

		// 增加已完成计数
		completed := reExec.phase1Completed.Add(1)

		// 检查是否所有交易都已处理
		if int(completed) == reExec.phase1Total {
			// 最后一个完成的线程负责初始化阶段二
			pe.finalizeReExecutePhase1(state, workerID)
		}
	}

	// 等待阶段一完成
	for !reExec.phase1Done.Load() {
		// busy wait
	}

	// 进入阶段二
	pe.workerStaties[workerID].Phase = SerialExecutePhase
}

// finalizeReExecutePhase1 完成重执行阶段一，初始化阶段二
func (pe *PipelineEngine) finalizeReExecutePhase1(state *BatchState, workerID int) {
	reExec := state.reExecute

	// 打印阶段一结果
	fmt.Printf("\n========== 重执行阶段一完成 (Batch %d) ==========\n", state.BatchID)
	fmt.Printf("重执行总数: %d\n", reExec.phase1Total)
	fmt.Printf("读写集未变（提交）: %d\n", len(reExec.phase1Committed))
	fmt.Printf("读写集变化（进入阶段二）: %d\n", len(reExec.phase1Aborted))

	// 打印提交的交易ID
	if len(reExec.phase1Committed) > 0 {
		fmt.Printf("阶段一提交交易: ")
		for _, rwset := range reExec.phase1Committed {
			fmt.Printf("%d ", rwset.TxID)
		}
		fmt.Println()
	}

	// 打印进入阶段二的交易ID
	if len(reExec.phase1Aborted) > 0 {
		fmt.Printf("阶段二待执行交易: ")
		for _, rwset := range reExec.phase1Aborted {
			fmt.Printf("%d ", rwset.TxID)
		}
		fmt.Println()
	}
	fmt.Printf("================================================\n\n")

	// 将阶段一提交的交易加入最终提交列表
	state.mu.Lock()
	state.CommittedTxs = append(state.CommittedTxs, reExec.phase1Committed...)
	state.mu.Unlock()

	// 准备阶段二队列（按原始顺序排序）
	if len(reExec.phase1Aborted) > 0 {
		sortedAborted := make([]*ReadWriteSet, len(reExec.phase1Aborted))
		copy(sortedAborted, reExec.phase1Aborted)
		sortByTxID(sortedAborted)
		reExec.phase2Queue = sortedAborted
	}

	// 标记阶段一完成
	reExec.phase1Done.Store(true)
}

// compareRWSet 比较两个读写集是否相同（只比较涉及冲突的部分）
func (pe *PipelineEngine) compareRWSet(old, new *ReadWriteSet) bool {
	// 比较读集
	if len(old.ReadSet) != len(new.ReadSet) {
		return false
	}
	for addr := range old.ReadSet {
		if _, exists := new.ReadSet[addr]; !exists {
			return false
		}
	}

	// 比较写集
	if len(old.WriteSet) != len(new.WriteSet) {
		return false
	}
	for addr := range old.WriteSet {
		if _, exists := new.WriteSet[addr]; !exists {
			return false
		}
	}

	return true
}

// sortByTxID 按交易ID排序
func sortByTxID(rwsets []*ReadWriteSet) {
	n := len(rwsets)
	for i := 0; i < n-1; i++ {
		for j := i + 1; j < n; j++ {
			if rwsets[i].TxID > rwsets[j].TxID {
				rwsets[i], rwsets[j] = rwsets[j], rwsets[i]
			}
		}
	}
}

// reExecutePhase2 重执行阶段二：串行执行
// 单线程按原始顺序串行执行阶段一中读写集变化的交易
func (pe *PipelineEngine) reExecutePhase2(state *BatchState, workerID int) {
	reExec := state.reExecute
	if reExec == nil {
		pe.reEntryWaitingTaskPhase(state, workerID)
		return
	}

	// 如果没有阶段二的交易，直接完成
	if len(reExec.phase2Queue) == 0 {
		// 尝试成为完成批次的线程
		if reExec.phase2Executor.CompareAndSwap(-1, int32(workerID)) {
			pe.finalizeReExecutePhase2(state, workerID)
		}

		// 等待阶段二完成
		for !reExec.phase2Done.Load() {
			// busy wait
		}
		pe.reEntryWaitingTaskPhase(state, workerID)
		return
	}

	// 尝试成为串行执行的线程（只有一个线程能执行）
	if !reExec.phase2Executor.CompareAndSwap(-1, int32(workerID)) {
		// 不是执行线程，等待阶段二完成
		for !reExec.phase2Done.Load() {
			// busy wait
		}
		pe.reEntryWaitingTaskPhase(state, workerID)
		return
	}

	// 当前线程负责串行执行
	fmt.Printf("\n[Worker %d] [ReExecute Phase2] 开始串行执行 %d 笔交易\n", workerID, len(reExec.phase2Queue))

	for i, oldRWSet := range reExec.phase2Queue {
		fmt.Printf("[Worker %d] [ReExecute Phase2] 串行执行交易 %d (%d/%d)\n",
			workerID, oldRWSet.TxID, i+1, len(reExec.phase2Queue))

		// 串行执行交易
		newRWSet := pe.reExecuteTransaction(oldRWSet, workerID)
		reExec.phase2Committed = append(reExec.phase2Committed, newRWSet)
	}

	// 完成阶段二
	pe.finalizeReExecutePhase2(state, workerID)
}

// finalizeReExecutePhase2 完成重执行阶段二，完成整个批次
func (pe *PipelineEngine) finalizeReExecutePhase2(state *BatchState, workerID int) {
	reExec := state.reExecute

	// 打印阶段二结果
	fmt.Printf("\n========== 重执行阶段二完成 (Batch %d) ==========\n", state.BatchID)
	fmt.Printf("串行执行交易数: %d\n", len(reExec.phase2Committed))

	if len(reExec.phase2Committed) > 0 {
		fmt.Printf("阶段二提交交易: ")
		for _, rwset := range reExec.phase2Committed {
			fmt.Printf("%d ", rwset.TxID)
		}
		fmt.Println()
	}
	fmt.Printf("================================================\n\n")

	// 将阶段二提交的交易加入最终提交列表
	state.mu.Lock()
	state.CommittedTxs = append(state.CommittedTxs, reExec.phase2Committed...)
	// 清空中止列表（所有交易都已处理）
	state.AbortedTxs = []*ReadWriteSet{}
	state.mu.Unlock()

	// 打印最终结果
	state.mu.Lock()
	totalCommitted := len(state.CommittedTxs)
	state.mu.Unlock()

	fmt.Printf("\n========== 批次 %d 最终结果 ==========\n", state.BatchID)
	fmt.Printf("最终提交交易数: %d\n", totalCommitted)
	fmt.Printf("最终中止交易数: 0\n")
	fmt.Printf("========================================\n\n")

	pe.reEntryWaitingTaskPhase(state, workerID)
	// 标记阶段二完成
	reExec.phase2Done.Store(true)
}
