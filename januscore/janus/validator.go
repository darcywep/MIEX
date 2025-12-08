package janus

import (
	"Janus/ethereum/core/types"
)

// ReadWriteSet 交易的读写集
type ReadWriteSet struct {
	TxID     int
	Tx       *types.Transaction
	ReadSet  map[string]struct{} // 读集合
	WriteSet map[string]struct{} // 写集合
	Cost     uint64              // 执行成本
	ThreadID int                 // 执行该交易的线程ID
}

// ConflictDAG 冲突有向无环图
type ConflictDAG struct {
	Nodes    map[int]*ReadWriteSet // 节点：交易ID -> 读写集
	Edges    map[int][]int         // 边：交易ID -> 依赖的交易ID列表
	InDegree map[int]int           // 入度
}

// StateTable 状态读写表
type StateTable struct {
	Address    string
	Operations []Operation // 读写操作列表（按交易ID递增）
}

// Operation 读写操作
type Operation struct {
	TxID int
	Type string // "r" for read, "w" for write
}

// ValidationResult 验证结果
type ValidationResult struct {
	CommittedTxs []*ReadWriteSet
	AbortedTxs   []*ReadWriteSet
}

// buildConflictDAG 构建冲突图 DAG
func (pe *PipelineEngine) buildConflictDAG(rwsets []*ReadWriteSet, threadRWSets map[int][]*ReadWriteSet) *ConflictDAG {
	dag := &ConflictDAG{
		Nodes:    make(map[int]*ReadWriteSet),
		Edges:    make(map[int][]int),
		InDegree: make(map[int]int),
	}

	// 初始化节点
	for _, rwset := range rwsets {
		dag.Nodes[rwset.TxID] = rwset
		dag.Edges[rwset.TxID] = make([]int, 0)
		dag.InDegree[rwset.TxID] = 0
	}

	// 构建每个线程的状态表
	threadStateTables := make(map[int]map[string]*StateTable)
	for threadID, threadRWs := range threadRWSets {
		threadStateTables[threadID] = pe.buildStateTable(threadRWs)
	}

	// 合并状态表，构建冲突边
	pe.mergeStateTables(threadStateTables, dag)

	return dag
}

// buildStateTable 构建单个线程的状态读写表
func (pe *PipelineEngine) buildStateTable(rwsets []*ReadWriteSet) map[string]*StateTable {
	stateMap := make(map[string]*StateTable)

	for _, rwset := range rwsets {
		// 处理读集
		for addr := range rwset.ReadSet {
			if stateMap[addr] == nil {
				stateMap[addr] = &StateTable{
					Address:    addr,
					Operations: make([]Operation, 0),
				}
			}
			stateMap[addr].Operations = append(stateMap[addr].Operations, Operation{
				TxID: rwset.TxID,
				Type: "r",
			})
		}

		// 处理写集
		for addr := range rwset.WriteSet {
			if stateMap[addr] == nil {
				stateMap[addr] = &StateTable{
					Address:    addr,
					Operations: make([]Operation, 0),
				}
			}
			stateMap[addr].Operations = append(stateMap[addr].Operations, Operation{
				TxID: rwset.TxID,
				Type: "w",
			})
		}
	}

	return stateMap
}

// mergeStateTables 合并多个线程的状态表
func (pe *PipelineEngine) mergeStateTables(threadStateTables map[int]map[string]*StateTable, dag *ConflictDAG) {
	// 获取所有状态地址
	addressSet := make(map[string]bool)
	for _, stateMap := range threadStateTables {
		for addr := range stateMap {
			addressSet[addr] = true
		}
	}

	// 对每个状态地址进行合并
	for addr := range addressSet {
		// 收集所有线程对该地址的操作
		allOps := make([]Operation, 0)
		for _, stateMap := range threadStateTables {
			if table := stateMap[addr]; table != nil {
				allOps = append(allOps, table.Operations...)
			}
		}

		// 构建冲突边
		var lastWrite *Operation
		for i := range allOps {
			op := &allOps[i]

			if op.Type == "w" {
				if lastWrite != nil {
					pe.addEdge(dag, lastWrite.TxID, op.TxID)
				}
				lastWrite = op
			} else if op.Type == "r" {
				if lastWrite != nil {
					pe.addEdge(dag, lastWrite.TxID, op.TxID)
				}
			}
		}
	}
}

// addEdge 添加边到DAG
func (pe *PipelineEngine) addEdge(dag *ConflictDAG, from, to int) {
	// 避免重复边
	for _, existTo := range dag.Edges[from] {
		if existTo == to {
			return
		}
	}

	dag.Edges[from] = append(dag.Edges[from], to)
	dag.InDegree[to]++
}

// extractSubDAGs 提取所有独立的子DAG
func (pe *PipelineEngine) extractSubDAGs(dag *ConflictDAG) []*ConflictDAG {
	visited := make(map[int]bool)
	subDAGs := make([]*ConflictDAG, 0)

	for nodeID := range dag.Nodes {
		if !visited[nodeID] {
			subDAG := pe.extractSubDAG(dag, nodeID, visited)
			if len(subDAG.Nodes) > 0 {
				subDAGs = append(subDAGs, subDAG)
			}
		}
	}

	return subDAGs
}

// extractSubDAG 从指定节点提取子DAG
func (pe *PipelineEngine) extractSubDAG(dag *ConflictDAG, startNode int, visited map[int]bool) *ConflictDAG {
	subDAG := &ConflictDAG{
		Nodes:    make(map[int]*ReadWriteSet),
		Edges:    make(map[int][]int),
		InDegree: make(map[int]int),
	}

	// BFS 遍历
	queue := []int{startNode}
	visited[startNode] = true

	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]

		subDAG.Nodes[nodeID] = dag.Nodes[nodeID]
		subDAG.Edges[nodeID] = dag.Edges[nodeID]
		subDAG.InDegree[nodeID] = dag.InDegree[nodeID]

		// 添加所有相邻节点
		for _, neighbor := range dag.Edges[nodeID] {
			if !visited[neighbor] {
				visited[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}

	return subDAG
}

// solveSubDAG 使用动态规划求解单个子DAG的最大提交集
func (pe *PipelineEngine) solveSubDAG(dag *ConflictDAG) []*ReadWriteSet {
	// 拓扑排序
	sorted := pe.topologicalSort(dag)
	if len(sorted) == 0 {
		return []*ReadWriteSet{}
	}

	// 动态规划
	n := len(sorted)
	dp := make([]uint64, n+1)
	choice := make([]bool, n)

	// 从后往前计算
	for i := n - 1; i >= 0; i-- {
		txID := sorted[i]
		cost := dag.Nodes[txID].Cost

		// 找到下一个不冲突的交易
		nextIdx := pe.findNextNonConflict(dag, sorted, i)

		// 选择提交或不提交
		notCommit := dp[i+1]
		commit := cost
		if nextIdx <= n {
			commit += dp[nextIdx]
		}

		if commit >= notCommit {
			dp[i] = commit
			choice[i] = true
		} else {
			dp[i] = notCommit
			choice[i] = false
		}
	}

	// 回溯选择的交易
	committed := make([]*ReadWriteSet, 0)
	for i := 0; i < n; i++ {
		if choice[i] {
			txID := sorted[i]
			committed = append(committed, dag.Nodes[txID])
		}
	}

	return committed
}

// topologicalSort 拓扑排序
func (pe *PipelineEngine) topologicalSort(dag *ConflictDAG) []int {
	inDegree := make(map[int]int)
	for id, degree := range dag.InDegree {
		inDegree[id] = degree
	}

	queue := make([]int, 0)
	for id, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}

	sorted := make([]int, 0)
	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		sorted = append(sorted, nodeID)

		for _, neighbor := range dag.Edges[nodeID] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	return sorted
}

// findNextNonConflict 找到下一个不冲突的交易索引
func (pe *PipelineEngine) findNextNonConflict(dag *ConflictDAG, sorted []int, currentIdx int) int {
	currentTxID := sorted[currentIdx]

	// 找到所有直接冲突的交易
	conflicts := make(map[int]bool)
	for _, conflictID := range dag.Edges[currentTxID] {
		conflicts[conflictID] = true
	}

	// 找到第一个不冲突的交易
	for i := currentIdx + 1; i < len(sorted); i++ {
		if !conflicts[sorted[i]] {
			return i
		}
	}

	return len(sorted)
}

// partitionByConflict 根据冲突关系将交易分组
func (pe *PipelineEngine) partitionByConflict(rwsets []*ReadWriteSet) [][]*ReadWriteSet {
	n := len(rwsets)
	if n == 0 {
		return [][]*ReadWriteSet{}
	}

	// 使用并查集分组
	parent := make([]int, n)
	for i := range parent {
		parent[i] = i
	}

	var find func(int) int
	find = func(x int) int {
		if parent[x] != x {
			parent[x] = find(parent[x])
		}
		return parent[x]
	}

	union := func(x, y int) {
		px, py := find(x), find(y)
		if px != py {
			parent[px] = py
		}
	}

	// 检查所有交易对的冲突关系
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if pe.hasConflict(rwsets[i], rwsets[j]) {
				union(i, j)
			}
		}
	}

	// 按根节点分组
	groups := make(map[int][]*ReadWriteSet)
	for i, rwset := range rwsets {
		root := find(i)
		if groups[root] == nil {
			groups[root] = make([]*ReadWriteSet, 0)
		}
		groups[root] = append(groups[root], rwset)
	}

	// 转换为切片
	result := make([][]*ReadWriteSet, 0, len(groups))
	for _, group := range groups {
		result = append(result, group)
	}

	return result
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
