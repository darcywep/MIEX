package janus

import (
	"fmt"
)

// buildStateTable 构建单个线程的状态读写表
func (pe *PipelineEngine) buildStateTable(state *BatchState, workerID int) map[string]*StateTable {
	stateMap := make(map[string]*StateTable)
	state.threadWriteSet[workerID] = make(map[string]struct{})

	for _, rwset := range state.ThreadRWSets[workerID] {
		// 处理读集
		for addr := range rwset.ReadSet {
			if stateMap[addr] == nil {
				stateMap[addr] = &StateTable{
					Address:    addr,
					Operations: make([]*Operation, 0),
				}
			}
			stateMap[addr].Operations = append(stateMap[addr].Operations, &Operation{
				TxID: rwset.TxID,
				Type: "r",
			})
		}

		// 处理写集
		for addr := range rwset.WriteSet {
			if stateMap[addr] == nil {
				stateMap[addr] = &StateTable{
					Address:    addr,
					Operations: make([]*Operation, 0),
				}
			}
			if _, exist := state.threadWriteSet[workerID][addr]; !exist {
				state.threadWriteSet[workerID][addr] = struct{}{}
			}
			stateMap[addr].Operations = append(stateMap[addr].Operations, &Operation{
				TxID: rwset.TxID,
				Type: "w",
			})
		}
	}

	// 对每个地址的操作列表进行排序
	// 排序规则：先按 TxID 排序，TxID 相同时 r 在前，w 在后
	for _, table := range stateMap {
		ops := table.Operations
		// 冒泡排序
		for i := 0; i < len(ops); i++ {
			for j := i + 1; j < len(ops); j++ {
				// 比较规则
				needSwap := false
				if ops[i].TxID > ops[j].TxID {
					// TxID 大的排后面
					needSwap = true
				} else if ops[i].TxID == ops[j].TxID {
					// TxID 相同，w 排后面（r 在前）
					if ops[i].Type == "w" && ops[j].Type == "r" {
						needSwap = true
					}
				}

				if needSwap {
					ops[i], ops[j] = ops[j], ops[i]
				}
			}
		}
	}

	return stateMap
}

// compareOperations 比较两个操作的顺序
// 返回值：< 0 表示 op1 < op2，0 表示相等，> 0 表示 op1 > op2
// 排序规则：先按 TxID，TxID 相同时 r 在前 w 在后
func (pe *PipelineEngine) compareOperations(op1, op2 *Operation) int {
	if op1.TxID < op2.TxID {
		return -1
	}
	if op1.TxID > op2.TxID {
		return 1
	}
	// TxID 相同，比较类型
	if op1.Type == "r" && op2.Type == "w" {
		return -1 // r 在前
	}
	if op1.Type == "w" && op2.Type == "r" {
		return 1 // w 在后
	}
	return 0 // 相同
}

// mergeTwoLists 合并两个已排序的操作列表(需要注意的是：某个线程可能不涉及该地址的读写操作，导致传入的列表数量可能少于2)
// 每个列表已经按 TxID 排序，TxID 相同时 r 在前 w 在后
func (pe *PipelineEngine) mergeTwoLists(lists [][]*Operation) []*Operation {
	if len(lists) == 0 {
		fmt.Println("[Error!] [mergeOperationLists] No operation lists to merge")
		return []*Operation{}
	}
	if len(lists) == 1 {
		return lists[0]
	}
	// 仅支持合并两个列表
	list1, list2 := lists[0], lists[1]
	result := make([]*Operation, 0, len(list1)+len(list2))
	i, j := 0, 0

	for i < len(list1) && j < len(list2) {
		// 比较两个操作，决定谁排在前面
		if pe.compareOperations(list1[i], list2[j]) <= 0 {
			result = append(result, list1[i])
			i++
		} else {
			result = append(result, list2[j])
			j++
		}
	}

	// 添加剩余元素
	for i < len(list1) {
		result = append(result, list1[i])
		i++
	}
	for j < len(list2) {
		result = append(result, list2[j])
		j++
	}

	return result
}

func (pe *PipelineEngine) mergeStateTables(state *BatchState, threadStateTable1, threadStateTable2 map[string]*StateTable, workerID int) (mergeThreadStateTable, pairStateTable map[string]*StateTable) {
	// 获取所有状态地址
	mergeThreadStateTable = make(map[string]*StateTable)
	for addr := range threadStateTable1 {
		mergeThreadStateTable[addr] = &StateTable{Address: addr}
	}
	for addr := range threadStateTable2 {
		if mergeThreadStateTable[addr] == nil {
			mergeThreadStateTable[addr] = &StateTable{Address: addr}
		}
	}

	// 对每个状态地址进行合并，构建冲突边
	for addr := range mergeThreadStateTable {
		// 收集两个线程对该地址的操作列表
		opLists := make([][]*Operation, 0)
		if table1 := threadStateTable1[addr]; table1 != nil {
			opLists = append(opLists, table1.Operations)
		}
		if table2 := threadStateTable2[addr]; table2 != nil {
			opLists = append(opLists, table2.Operations)
		}

		// 使用归并排序合并两个已排序的操作列表
		mergeThreadStateTable[addr].Operations = pe.mergeTwoLists(opLists)
	}
	state.MergeThreadStateTables.queueMu.Lock()

	state.MergeThreadStateTables.completedMergeCount++
	completedMergeCount := state.MergeThreadStateTables.completedMergeCount // 先拿出merge的数量，释放锁后再使用，避免堵塞
	if completedMergeCount%2 == state.MergeThreadStateTables.waitFlag {     // 需要进入等待
		state.MergeThreadStateTables.stateTablesQueue = append(state.MergeThreadStateTables.stateTablesQueue, mergeThreadStateTable)
	} else { // 偶数次合并，直接进行合并
		pairStateTable = state.MergeThreadStateTables.stateTablesQueue[0]
		state.MergeThreadStateTables.stateTablesQueue = state.MergeThreadStateTables.stateTablesQueue[1:]
	}
	state.MergeThreadStateTables.queueMu.Unlock()

	isWait := state.MergeThreadStateTables.awakeOrWaitThreadStateTableForMerge(state, completedMergeCount, workerID)
	if isWait {
		// 被唤醒之后进入下一阶段
		pe.workerStaties[workerID].Phase = ConstructDAGPhase
	} // 完成之后，未睡眠，仍然在当前阶段
	return mergeThreadStateTable, pairStateTable
}

// groupOperationsByTx 将操作按交易分组
// 输入：[(tx1,r), (tx1,w), (tx2,r), (tx2,w), (tx3,w)]
// 输出：[{tx1, hasR, hasW}, {tx2, hasR, hasW}, {tx3, noR, hasW}]
func (pe *PipelineEngine) groupOperationsByTx(ops []*Operation) []*TxOperation {
	if len(ops) == 0 {
		return []*TxOperation{}
	}

	result := make([]*TxOperation, 0)
	currentTxID := ops[0].TxID
	currentOp := &TxOperation{TxID: currentTxID}

	for _, op := range ops {
		if op.TxID != currentTxID {
			// 新的交易，保存当前交易操作
			result = append(result, currentOp)
			currentTxID = op.TxID
			currentOp = &TxOperation{TxID: currentTxID}
		}

		// 记录操作类型
		if op.Type == "r" {
			currentOp.HasRead = true
		} else if op.Type == "w" {
			currentOp.HasWrite = true
		}
	}

	// 添加最后一个交易
	result = append(result, currentOp)
	return result
}

// buildConflictEdges 构建冲突边
// 规则：
// 1. 同一交易的读写操作视为一个整体，只产生一条边
// 2. 传递性冲突：如果 tx_i 有写，tx_j 有读，则 tx_i → tx_j
// 3. 边的类型始终为 WR（简化处理，用于最大提交验证）
func (pe *PipelineEngine) buildConflictEdges(txOps []*TxOperation, addr string, dag *ConflictDAG) {
	n := len(txOps)
	if n <= 1 {
		return
	}

	// 对于每对交易 (i, j)，如果 i < j 且 tx_i 有写，tx_j 有读，则添加边
	for i := 0; i < n; i++ {
		if !txOps[i].HasWrite {
			continue // tx_i 没有写操作，不会产生冲突边
		}

		for j := i + 1; j < n; j++ {
			// tx_i 有写，tx_j 有读，产生冲突
			if txOps[j].HasRead {
				// 所有边类型都标记为 WR（用于最大提交验证）
				//pe.addEdgeWithDetail(dag, txOps[i].TxID, txOps[j].TxID, addr, "WR")
				fromTxID := txOps[i].TxID
				toTxID := txOps[j].TxID

				// 检查边是否已存在，避免重复添加
				if !pe.hasEdge(dag, fromTxID, toTxID) {
					pe.addEdge(dag, fromTxID, toTxID)
				}
			}
		}
	}
}

// hasEdge 检查两个节点之间是否已存在边（无向）
func (pe *PipelineEngine) hasEdge(dag *ConflictDAG, from, to int) bool {
	// 只需检查一个方向，因为无向图是对称的
	if toSet, exists := dag.Edges[from]; exists {
		if _, hasEdge := toSet[to]; hasEdge {
			return true
		}
	}
	return false
}

// addEdge 添加边到DAG
// 参数：
//   - from: 源交易ID
//   - to: 目标交易ID
func (pe *PipelineEngine) addEdge(dag *ConflictDAG, from, to int) {
	//// 初始化 from 的边集合（如果不存在）
	//if dag.Edges[from] == nil {
	//	dag.Edges[from] = make(map[int]struct{})
	//}
	//
	//// 添加边 from -> to
	//dag.Edges[from][to] = struct{}{}
	//
	//// 增加 to 的入度
	//dag.InDegree[to]++

	// 初始化 from 的边集合（如果不存在）
	if dag.Edges[from] == nil {
		dag.Edges[from] = make(map[int]struct{})
	}
	// 初始化 to 的边集合（如果不存在）
	if dag.Edges[to] == nil {
		dag.Edges[to] = make(map[int]struct{})
	}

	// 添加双向边 from <-> to
	dag.Edges[from][to] = struct{}{}
	dag.Edges[to][from] = struct{}{}

	// 无向图：增加两个节点的度数
	dag.Degree[from]++
	dag.Degree[to]++

	rootFromBefore := dag.Find(from)
	rootToBefore := dag.Find(to)
	// 合并连通分量
	dag.Union(from, to)

	rootAfter := dag.Find(from)
	fmt.Printf("[AddEdge] 添加边 (%d, %d), 合并前: root(%d)=%d, root(%d)=%d, 合并后: root=%d\n",
		from, to, from, rootFromBefore, to, rootToBefore, rootAfter)
}

// TxOperation 表示一个交易对某个地址的所有操作
type TxOperation struct {
	TxID     int
	HasRead  bool
	HasWrite bool
}

// constructDAGForAddress 为每个地址构建DAG的冲突边
// 参数:
//   - state: 当前批次状态
//   - rwTable1: 第一个状态读写表
//   - rwTable2: 第二个状态读写表（可能为nil，表示只有一个表）
//   - workerID: 当前工作线程ID
//
// 功能：
// 1. 处理一个或两个StateTable中的操作
// 2. 按交易分组操作
// 3. 根据冲突规则构建DAG边
// 4. 将交易节点添加到DAG中
func (pe *PipelineEngine) constructDAGForAddress(state *BatchState, rwTable1, rwTable2 *StateTable, workerID int) {
	// 获取当前工作线程的DAG
	dag := state.constructDAG.dags[workerID]

	constructDAG := func(rwTable *StateTable) {
		if rwTable != nil && len(rwTable.Operations) > 0 {
			// 步骤1：将操作按交易分组
			// 输入: [(tx1,r), (tx1,w), (tx2,r), ...]
			// 输出: [{tx1, hasR, hasW}, {tx2, hasR, noW}, ...]
			txOps := pe.groupOperationsByTx(rwTable.Operations)

			// 步骤2：将涉及的交易节点添加到 DAG
			for _, txOp := range txOps {
				// 如果节点尚未添加到DAG中
				if _, exists := dag.Nodes[txOp.TxID]; !exists {
					// 从批次状态中查找对应的交易读写集
					rwset := pe.janusTransactions[txOp.TxID].rwSet
					if rwset == nil {
						panic(fmt.Errorf("dag.Nodes[%d].rwSet is nil", txOp.TxID))
					}
					dag.Nodes[txOp.TxID] = rwset // 添加节点
					//dag.EdgeDetails[txOp.TxID] = make(map[int]*ConflictEdge)
					dag.Edges[txOp.TxID] = make(map[int]struct{})
					dag.Degree[txOp.TxID] = 0
				}
			}

			// 步骤3：根据冲突规则构建冲突边
			// 规则：如果 tx_i 有写，tx_j (j>i) 有读或写，则添加边 tx_i → tx_j
			pe.buildConflictEdges(txOps, rwTable.Address, dag)

		}
	}
	// ===== 处理第一个状态表 =====
	constructDAG(rwTable1)

	// ===== 处理第二个状态表（如果存在）=====
	constructDAG(rwTable2)

	// 日志：当前 DAG 状态
	components := dag.GetConnectedComponents()
	fmt.Printf("[Worker %d] [ConstructDAG] 当前DAG: 节点数=%d, 连通分量数=%d\n",
		workerID, len(dag.Nodes), len(components))
}

func (pe *PipelineEngine) mergeTwoDags(state *BatchState, pairDag *ConflictDAG, workerID int) (newPairDag *ConflictDAG) {
	// 获取当前工作线程的DAG
	dag := state.constructDAG.dags[workerID]
	// 日志：合并前的状态
	fmt.Printf("[Worker %d] [MergeDag] 开始合并, myDag节点数=%d, pairDag节点数=%d\n",
		workerID, len(dag.Nodes), len(pairDag.Nodes))

	for nodeID, rwset := range pairDag.Nodes {
		// 如果节点尚未添加到DAG中
		if _, exists := dag.Nodes[nodeID]; !exists {
			dag.Nodes[nodeID] = rwset // 添加节点
			dag.Edges[nodeID] = pairDag.Edges[nodeID]
			dag.Degree[nodeID] = pairDag.Degree[nodeID]
			continue
		}

		// 节点已存在，合并边集合（无向图）
		// 遍历pairDag中该节点的所有邻接边
		for toNodeID := range pairDag.Edges[nodeID] {
			// 如果边不存在，添加双向边
			if !pe.hasEdge(dag, nodeID, toNodeID) {
				// 确保目标节点的边集合已初始化
				if dag.Edges[toNodeID] == nil {
					dag.Edges[toNodeID] = make(map[int]struct{})
				}
				// 添加双向边
				dag.Edges[nodeID][toNodeID] = struct{}{}
				dag.Edges[toNodeID][nodeID] = struct{}{}
				// 增加度数
				dag.Degree[nodeID]++
				dag.Degree[toNodeID]++
				// 合并连通分量
				//dag.Union(nodeID, toNodeID)
			}
		}
	}

	// 合并pairDAG中的连通分量信息
	// 遍历pairDAG中的所有边，确保正确合并
	for nodeID := range pairDag.Nodes {
		for toNodeID := range pairDag.Edges[nodeID] {
			dag.Union(nodeID, toNodeID)
		}
	}
	// 日志：合并后的连通分量
	components := dag.GetConnectedComponents()
	fmt.Printf("[Worker %d] [MergeDag] 合并完成, 总节点数=%d, 连通分量数=%d\n",
		workerID, len(dag.Nodes), len(components))
	for root, nodes := range components {
		fmt.Printf("[Worker %d] [MergeDag]   连通分量 root=%d: %v\n", workerID, root, nodes)
	}

	if pairDag.totalMerges != -1 {
		dag.totalMerges = pairDag.totalMerges
	}

	state.constructDAG.queueMu.Lock()
	state.constructDAG.completedMergeCount++
	completedMergeCount := state.constructDAG.completedMergeCount
	if completedMergeCount%2 == 1 { // 需要进入等待
		state.constructDAG.dagQueue = append(state.constructDAG.dagQueue, dag)
	} else {
		// 偶数次合并，直接进行合并
		newPairDag = state.constructDAG.dagQueue[0]
		state.constructDAG.dagQueue = state.constructDAG.dagQueue[1:]
	}
	state.constructDAG.queueMu.Unlock()
	isWait := state.constructDAG.awakeOrWaitConstructDAG(state, completedMergeCount, dag.totalMerges, workerID)
	if isWait {
		// 被唤醒之后进入下一阶段
		pe.workerStaties[workerID].Phase = CommitMaximumValidationPhase
	} // 完成之后，未睡眠，仍然在当前阶段

	return newPairDag
}
