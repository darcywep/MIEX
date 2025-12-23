package janus

//
//// extractSubDAGs 提取所有独立的子DAG
//func (pe *PipelineEngine) extractSubDAGs(dag *ConflictDAG) []*ConflictDAG {
//	visited := make(map[int]bool)
//	subDAGs := make([]*ConflictDAG, 0)
//
//	for nodeID := range dag.Nodes {
//		if !visited[nodeID] {
//			subDAG := pe.extractSubDAG(dag, nodeID, visited)
//			if len(subDAG.Nodes) > 0 {
//				subDAGs = append(subDAGs, subDAG)
//			}
//		}
//	}
//
//	return subDAGs
//}
//
//// extractSubDAG 从指定节点提取子DAG
//func (pe *PipelineEngine) extractSubDAG(dag *ConflictDAG, startNode int, visited map[int]bool) *ConflictDAG {
//	subDAG := &ConflictDAG{
//		Nodes:    make(map[int]*ReadWriteSet),
//		Edges:    make(map[int][]int),
//		InDegree: make(map[int]int),
//	}
//
//	// BFS 遍历
//	queue := []int{startNode}
//	visited[startNode] = true
//
//	for len(queue) > 0 {
//		nodeID := queue[0]
//		queue = queue[1:]
//
//		subDAG.Nodes[nodeID] = dag.Nodes[nodeID]
//		subDAG.Edges[nodeID] = dag.Edges[nodeID]
//		subDAG.InDegree[nodeID] = dag.InDegree[nodeID]
//
//		// 添加所有相邻节点
//		for _, neighbor := range dag.Edges[nodeID] {
//			if !visited[neighbor] {
//				visited[neighbor] = true
//				queue = append(queue, neighbor)
//			}
//		}
//	}
//
//	return subDAG
//}
//
//// solveSubDAG 使用动态规划求解单个子DAG的最大提交集
//func (pe *PipelineEngine) solveSubDAG(dag *ConflictDAG) []*ReadWriteSet {
//	// 拓扑排序
//	sorted := pe.topologicalSort(dag)
//	if len(sorted) == 0 {
//		return []*ReadWriteSet{}
//	}
//
//	// 动态规划
//	n := len(sorted)
//	dp := make([]uint64, n+1)
//	choice := make([]bool, n)
//
//	// 从后往前计算
//	for i := n - 1; i >= 0; i-- {
//		txID := sorted[i]
//		cost := dag.Nodes[txID].Cost
//
//		// 找到下一个不冲突的交易
//		nextIdx := pe.findNextNonConflict(dag, sorted, i)
//
//		// 选择提交或不提交
//		notCommit := dp[i+1]
//		commit := cost
//		if nextIdx <= n {
//			commit += dp[nextIdx]
//		}
//
//		if commit >= notCommit {
//			dp[i] = commit
//			choice[i] = true
//		} else {
//			dp[i] = notCommit
//			choice[i] = false
//		}
//	}
//
//	// 回溯选择的交易
//	committed := make([]*ReadWriteSet, 0)
//	for i := 0; i < n; i++ {
//		if choice[i] {
//			txID := sorted[i]
//			committed = append(committed, dag.Nodes[txID])
//		}
//	}
//
//	return committed
//}
//
//// topologicalSort 拓扑排序
//func (pe *PipelineEngine) topologicalSort(dag *ConflictDAG) []int {
//	inDegree := make(map[int]int)
//	for id, degree := range dag.InDegree {
//		inDegree[id] = degree
//	}
//
//	queue := make([]int, 0)
//	for id, degree := range inDegree {
//		if degree == 0 {
//			queue = append(queue, id)
//		}
//	}
//
//	sorted := make([]int, 0)
//	for len(queue) > 0 {
//		nodeID := queue[0]
//		queue = queue[1:]
//		sorted = append(sorted, nodeID)
//
//		for _, neighbor := range dag.Edges[nodeID] {
//			inDegree[neighbor]--
//			if inDegree[neighbor] == 0 {
//				queue = append(queue, neighbor)
//			}
//		}
//	}
//
//	return sorted
//}
//
//// findNextNonConflict 找到下一个不冲突的交易索引
//func (pe *PipelineEngine) findNextNonConflict(dag *ConflictDAG, sorted []int, currentIdx int) int {
//	currentTxID := sorted[currentIdx]
//
//	// 找到所有直接冲突的交易
//	conflicts := make(map[int]bool)
//	for _, conflictID := range dag.Edges[currentTxID] {
//		conflicts[conflictID] = true
//	}
//
//	// 找到第一个不冲突的交易
//	for i := currentIdx + 1; i < len(sorted); i++ {
//		if !conflicts[sorted[i]] {
//			return i
//		}
//	}
//
//	return len(sorted)
//}
