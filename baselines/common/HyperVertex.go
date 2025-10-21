package common

import (
	"sync"
	"time"
)

type HyperVertex struct {
	mu sync.RWMutex

	HyperId    int
	IsNested   bool
	MinIn      int
	MinOut     int
	Cost       float64
	InCost     float64
	OutCost    float64
	Aborted    bool
	Setted     bool
	CommitTime time.Time

	RootVertex *Vertex

	// 索引结构
	InAllRB  map[*Vertex]bool
	OutAllRB map[*Vertex]int

	OutEdges    []map[*Vertex]bool
	InEdges     []map[*Vertex]bool
	InRollback  []map[*Vertex]bool
	OutRollback []map[*Vertex]bool
	OutWeights  []float64
	InWeights   []float64

	RollbackType EdgeType
	Vertices     map[*Vertex]bool

	// 单线程版本
	InAllRBS      map[*Vertex]bool
	OutEdgesS     map[*HyperVertex]map[*Vertex]bool
	InEdgesS      map[*HyperVertex]map[*Vertex]bool
	OutHvS        map[*HyperVertex]bool
	InHvS         map[*HyperVertex]bool
	OutMapS       []map[*Vertex]bool
	InMapS        []map[*Vertex]bool
	InRollbackMS  []map[*Vertex]bool
	OutRollbackMS []map[*Vertex]bool
	OutRollbackS  map[*HyperVertex]map[*Vertex]bool
}

func NewHyperVertex(id int, isNested bool) *HyperVertex {
	hv := &HyperVertex{
		HyperId:      id,
		IsNested:     isNested,
		MinIn:        int(^uint(0) >> 1),
		MinOut:       int(^uint(0) >> 1),
		InAllRB:      make(map[*Vertex]bool),
		OutAllRB:     make(map[*Vertex]int),
		Vertices:     make(map[*Vertex]bool),
		InAllRBS:     make(map[*Vertex]bool),
		OutEdgesS:    make(map[*HyperVertex]map[*Vertex]bool),
		InEdgesS:     make(map[*HyperVertex]map[*Vertex]bool),
		OutHvS:       make(map[*HyperVertex]bool),
		InHvS:        make(map[*HyperVertex]bool),
		OutRollbackS: make(map[*HyperVertex]map[*Vertex]bool),
	}

	// 初始化切片
	size := BLOCK_SIZE + 1
	hv.OutEdges = make([]map[*Vertex]bool, size)
	hv.InEdges = make([]map[*Vertex]bool, size)
	hv.InRollback = make([]map[*Vertex]bool, size)
	hv.OutRollback = make([]map[*Vertex]bool, size)
	hv.OutWeights = make([]float64, size)
	hv.InWeights = make([]float64, size)
	hv.OutMapS = make([]map[*Vertex]bool, size)
	hv.InMapS = make([]map[*Vertex]bool, size)
	hv.InRollbackMS = make([]map[*Vertex]bool, size)
	hv.OutRollbackMS = make([]map[*Vertex]bool, size)

	for i := range hv.OutEdges {
		hv.OutEdges[i] = make(map[*Vertex]bool)
		hv.InEdges[i] = make(map[*Vertex]bool)
		hv.InRollback[i] = make(map[*Vertex]bool)
		hv.OutRollback[i] = make(map[*Vertex]bool)
		hv.OutMapS[i] = make(map[*Vertex]bool)
		hv.InMapS[i] = make(map[*Vertex]bool)
		hv.InRollbackMS[i] = make(map[*Vertex]bool)
		hv.OutRollbackMS[i] = make(map[*Vertex]bool)
	}

	return hv
}

func (hv *HyperVertex) RecognizeCascades(vertex *Vertex) {
	for child := range vertex.Children {
		if child.Dependency == STRONG {
			child.Vertex.Cost = vertex.Cost
			// 复制级联顶点
			for cascade := range vertex.CascadeVertices {
				child.Vertex.CascadeVertices[cascade] = true
			}
		}
		hv.RecognizeCascades(child.Vertex)
	}
}

// // 构建普通事务节点
func (hv *HyperVertex) BuildVertexsSimple(tx *TPCCTransaction, vertex *Vertex, invertedIndex map[string]*RWSets) {
	// 获取执行时间
	execTime := tx.GetExecutionTime()
	vertex.SelfCost += execTime

	// 添加读写集
	readRows := tx.GetReadRows()
	for key, _ := range readRows {
		vertex.ReadSet[key] = true
		//vertex.AddToReadSet(readRow)
	}

	updateRows := tx.GetUpdateRows()
	for key, _ := range updateRows {
		//vertex.AddToWriteSet(updateRow)
		vertex.WriteSet[key] = true
	}

	// 构建倒排索引
	for readKey, _ := range readRows {
		if _, exists := invertedIndex[readKey]; !exists {
			invertedIndex[readKey] = NewRWSets()
		}
		invertedIndex[readKey].ReadSet[vertex] = true
	}

	for writeKey, _ := range updateRows {
		if _, exists := invertedIndex[writeKey]; !exists {
			invertedIndex[writeKey] = NewRWSets()
		}
		invertedIndex[writeKey].WriteSet[vertex] = true
	}

	//// 如果事务有子事务，则添加子事务读写集和执行时间
	//children := tx.GetChildren()
	//if len(children) > 0 {
	//	// 递归计算子节点权重 级联回滚权重和级联子节点
	//	for i := 0; i < len(children); i++ {
	//		// 递归添加级联回滚代价
	//		hv.BuildVertexsSimple(children[i].Transaction, vertex, invertedIndex)
	//	}
	//}

	//fmt.Printf("VertexId: %s Cost: %d\n", vertex.GetID(), vertex.GetCost())
}

func (hv *HyperVertex) BuildVertexs(tx *TPCCTransaction, hyperVertex *HyperVertex, vertex *Vertex, txid string, invertedIndex map[string]*RWSets) int {
	execTime := tx.GetExecutionTime()
	vertex.SelfCost = execTime

	if len(tx.GetChildren()) != 0 {
		vertex.IsNested = true
		children := tx.GetChildren()

		for i := 1; i <= len(children); i++ {
			child := children[i-1]
			subTxid := txid + "_" + string(rune(i))
			childVertex := NewVertex(hyperVertex, hv.HyperId, subTxid, vertex.Layer+1, true)

			execTime += hv.BuildVertexs(child.Transaction, hyperVertex, childVertex, subTxid, invertedIndex)

			if child.Dependency == STRONG {
				vertex.HasStrong = true
				vertex.StrongChildren[childVertex] = true
				childVertex.StrongParent = vertex
			}

			// 合并级联顶点
			for cascade := range childVertex.CascadeVertices {
				vertex.CascadeVertices[cascade] = true
			}

			vertex.AddChild(childVertex, child.Dependency)

			// 合并读写集
			for read := range childVertex.AllReadSet {
				vertex.AllReadSet[read] = true
			}
			for write := range childVertex.AllWriteSet {
				vertex.AllWriteSet[write] = true
			}
		}
	}

	// 添加读写集
	for read := range tx.GetReadRows() {
		vertex.ReadSet[read] = true
		vertex.AllReadSet[read] = true
	}
	for write := range tx.GetUpdateRows() {
		vertex.WriteSet[write] = true
		vertex.AllWriteSet[write] = true
	}

	// 构建倒排索引
	for read := range vertex.ReadSet {
		if invertedIndex[read] == nil {
			invertedIndex[read] = &RWSets{
				ReadSet:  make(map[*Vertex]bool),
				WriteSet: make(map[*Vertex]bool),
			}
		}
		invertedIndex[read].ReadSet[vertex] = true
	}
	for write := range vertex.WriteSet {
		if invertedIndex[write] == nil {
			invertedIndex[write] = &RWSets{
				ReadSet:  make(map[*Vertex]bool),
				WriteSet: make(map[*Vertex]bool),
			}
		}
		invertedIndex[write].WriteSet[vertex] = true
	}

	vertex.Cost = execTime
	hv.Vertices[vertex] = true
	return execTime
}

func (hv *HyperVertex) PrintVertexTree() {
	if hv.RootVertex != nil {
		hv.RootVertex.PrintVertex()
	}
}

func (hv *HyperVertex) SetCommitTime(commitTime time.Time) {
	hv.mu.Lock()
	defer hv.mu.Unlock()

	if hv.Setted {
		return
	}
	hv.CommitTime = commitTime
	hv.Setted = true
}
