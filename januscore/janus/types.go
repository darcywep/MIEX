package janus

import (
	"Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

// ReExecuteState 重执行状态
type ReExecuteState struct {
	// ============== 阶段一：并发重执行 ==================
	cg                       *ConflictDAG
	abortedTxComponents      [][]int // 按连通分量划分的丢交易 连通分量 -> 丢弃集
	abortedTxComponentsIndex atomic.Int32
	totalAbortedTxComponents int //总连通分量数量

	// 求解结果
	threadCommittedTxs [][]int // 按线程顺序存储的可提交交易列表，所有线程完成之后需要合并到 state.CommittedTxs

	completedIndex atomic.Int32

	abortedTxs       []int   // txId
	threadAbortedTxs [][]int // 线程 -> txId

	done atomic.Bool
}

// newReExecuteState 创建重执行状态
func newReExecuteState(cg *ConflictDAG, abortedTxComponents [][]int, threadNumber int) *ReExecuteState {
	res := &ReExecuteState{
		cg:                       cg,
		abortedTxComponents:      abortedTxComponents,
		totalAbortedTxComponents: len(abortedTxComponents),
		threadCommittedTxs:       make([][]int, threadNumber),
		abortedTxs:               make([]int, 0),
		threadAbortedTxs:         make([][]int, threadNumber),
	}
	return res
}

type janusTransaction struct {
	Tx            *types.Transaction
	rwSet         *ReadWriteSet
	ExecutionCost float64 // Estimated execution cost
	OriginalIdx   int

	IsLongTx      bool
	EarlyAbort    bool
	CheckConflict bool
	IsRuned       bool
}

var AllJanusTransactions [][]*janusTransaction

func init() {
	AllJanusTransactions = make([][]*janusTransaction, 0, config.AllThreadNum)
}

// ThreadStateTableForMerge
// 给定n个有序切片：
//
// 1. 总合并次数 = n - 1
//
// 2. 如果 n 是奇数：
//   - 第k次完成，k为偶数 → 应该wait
//   - 第k次完成，k为奇数 → 不应该wait
//
// 3. 如果 n 是偶数：
//   - 第k次完成，k为奇数 → 应该wait
//   - 第k次完成，k为偶数 → 不应该wait
//
// 4. 第(n-1)次完成 → Broadcast唤醒所有
type ThreadStateTableForMerge struct {
	condMu sync.Mutex
	cond   *sync.Cond

	queueMu          sync.Mutex
	stateTablesQueue []*stateTableWithWriteSet // 队列(状态地址 -> 状态读写表)

	// 核心：只需要这两个
	initialCount        int // 初始切片数量（判断奇偶）
	waitFlag            int // 如果初始数量为奇数，那么就为0，即偶数次完成的需要wait；如果初始数量为偶数，那么就为1，即奇数次完成的需要wait
	totalMerges         int // 需要合并的总次数 (n-1)
	completedMergeCount int // 已完成的合并次数

	//done bool
	done atomic.Bool
}

func (pe *PipelineEngine) awakeOrWaitThreadStateTableForMerge(state *BatchState, tstfm *ThreadStateTableForMerge, completedMergeCount int, workerID int) (isWait bool) {
	// 判断是否需要等待或唤醒
	if completedMergeCount%2 == tstfm.waitFlag { // 最后一次肯定是需要Wait的，所以这里需要判断是否为最后一次，如果是则唤醒
		// 需要等待
		if completedMergeCount < tstfm.totalMerges {
			// 不是最后一次合并，直接返回，到下一阶段忙等待
			if enableLog {
				fmt.Printf("[Worker %d] [batch %d]: merge state table %d/%d, try entry next phase.\n",
					workerID, state.BatchID, completedMergeCount, tstfm.totalMerges)
			}
		} else {
			// 最后一次合并，唤醒所有等待的
			for _, rwTable := range tstfm.stateTablesQueue[0].stateTables {
				state.constructDAG.stateTables = append(state.constructDAG.stateTables, rwTable)
			}
			state.writeSet = tstfm.stateTablesQueue[0].writeSet

			pe.timeOfMergeStateTablePhase += time.Since(state.startTimeOfMergeStateTablePhase)
			state.startTimeOfConstructDAGPhase = time.Now()
			tstfm.done.Store(true)

			if enableLog {
				fmt.Printf("[Worker %d] [batch %d]: merge state table %d/%d completed, broadcasting to all busy waiting...\n",
					workerID, state.BatchID, completedMergeCount, tstfm.totalMerges)
				fmt.Printf("[Worker %d] [batch %d] state table have access %d address\n", workerID, state.BatchID, len(tstfm.stateTablesQueue[0].stateTables))
			}
		}
		return true // 可以先休息一会，休息完继续做牛马
	}
	return false // 需要继续做牛马
}

func newThreadStateTableForMerge(n int) *ThreadStateTableForMerge {
	tstfm := &ThreadStateTableForMerge{
		stateTablesQueue: make([]*stateTableWithWriteSet, 0),
		initialCount:     n,
		totalMerges:      n - 1,
	}
	if n%2 == 0 {
		tstfm.waitFlag = 1 // 初始为偶数，奇数次完成需要wait
	} else {
		tstfm.waitFlag = 0 // 初始为奇数，偶数次完成需要wait
	}
	tstfm.cond = sync.NewCond(&tstfm.condMu)
	return tstfm
}

// 写集也需要像stateTable一样，进行合并
type stateTableWithWriteSet struct {
	stateTables map[string]*StateTable // 线程ID -> 状态地址 -> 状态读写表
	writeSet    map[string]struct{}
}

func newStateTableWithWriteSet() *stateTableWithWriteSet {
	return &stateTableWithWriteSet{
		stateTables: make(map[string]*StateTable),
		writeSet:    make(map[string]struct{}),
	}
}

// BatchState 批次状态
type BatchState struct {
	BatchID   int
	Batch     *Batch
	NextBatch *Batch

	// 交易队列
	LongTxs  []*janusTransaction
	ShortTxs []*janusTransaction

	// 执行索引（原子操作）
	LongTxIndex  atomic.Int32
	ShortTxIndex atomic.Int32

	NextLongTxIndex  atomic.Int32
	NextShortTxIndex atomic.Int32

	// 本批次的写集, 会影响下一批的执行
	// 即预执行是否需要丢弃
	writeSet map[string]struct{}

	// 每个线程的执行结果，用于验证阶段构图
	// 第一个下标是线程，第二个下标是该线程执行的交易列表，按线程的执行顺序存储
	ThreadRWSets [][]*ReadWriteSet

	// threadStateTables 每个线程的状态读写表，该线程完成该批次的交易后，需要先构建自己的状态读写表
	ThreadStateTables []*stateTableWithWriteSet // 线程ID -> 状态地址 -> 状态读写表
	//                                                    -> 写集

	// 合并后的状态读写表，用于验证阶段构图，所有线程完成该批次后合并
	// 当只有一个MergeThreadStateTables时，表示该线程完成了所有交易，无需合并
	MergeThreadStateTables *ThreadStateTableForMerge

	// 执行计数
	TotalTxs int

	// 线程完成顺序
	CompletionOrder  []int
	finishedTreadNum int
	pairIndex        int // 用于记录当前线程应该和CompletionOrder中的哪个位置配对，按序
	CompletionMu     sync.Mutex

	// 验证相关
	CommittedTxs []int

	// 构图相关
	constructDAG *constructDAGResult
	// 重执行相关
	reExecute *ReExecuteState

	// 时间统计
	startTimeOfExecuteCurrentBatchPhase     time.Time
	startTimeOfExecuteCurrentBatchTail      time.Time
	startTimeOfMergeStateTablePhase         time.Time
	startTimeOfConstructDAGPhase            time.Time
	startTimeOfCommitMaximumValidationPhase time.Time
	startTimeOfReExecutePhase               time.Time

	// 切换到下一批
	finished atomic.Bool // 本批次是否完成
}

// ReadWriteSet 交易的读写集
type ReadWriteSet struct {
	TxID       int
	Tx         *janusTransaction
	ReadSet    map[string]struct{} // 读集合
	WriteSet   map[string]struct{} // 写集合
	Cost       float64             // 执行成本
	ThreadID   int                 // 执行该交易的线程ID
	EarlyAbort bool                // 是否被 Early Abort
	Executed   bool                // 是否已执行（用于追踪提前执行的交易）
}

type constructDAGResult struct {
	condMu sync.Mutex
	cond   *sync.Cond

	//stateTableIndex  atomic.Int32     // 当前处理到的状态读写表索引
	//constructThreads []*atomic.Bool   // 哪些线程构建过DAG
	// 低 32 位：stateTableIndex
	// 高 32 位：constructThreads 位图
	packedState atomic.Uint64

	stateTables []*StateTable  // 有序的状态读写列表
	dags        []*ConflictDAG // 按线程顺序存储的DAG列表

	queueMu  sync.Mutex
	dagQueue []*ConflictDAG // 队列(待合并的DAG)

	// 核心：只需要这两个
	// 通过有多少个stateTable来判断所有线程是否完成
	completedMergeCount int // 已完成的合并次数

	//done bool
	done atomic.Bool

	// 最大权重独立集相关

	componentsQueue [][]int // 连通分量队列（待求解）
	componentsIndex atomic.Int32
	totalComponents int //总连通分量数量

	// 求解结果
	threadCommittedTxs [][]int // 按线程顺序存储的可提交交易列表，所有线程完成之后需要合并到 state.CommittedTxs

	completeComponentsNumber atomic.Int32

	abortedTxs       [][]int   // 按连通分量划分的丢交易 连通分量 -> 丢弃集
	threadAbortedTxs [][][]int // 线程 -> 连通分量 -> 丢弃集

	mwisDone atomic.Bool
}

func (pe *PipelineEngine) awakeOrWaitConstructDAG(state *BatchState, cdr *constructDAGResult, completedMergeCount, totalMerges int, workerID int) (isWait bool) {
	// 判断是否需要等待或唤醒
	// 最后一次肯定是需要Wait的，所以这里需要判断是否为最后一次，如果是则唤醒
	if completedMergeCount%2 == 1 { // 奇数次完成需要wait
		// 需要等待
		if completedMergeCount != totalMerges {
			// 不是最后一次合并，直接返回，到下一阶段忙等待
			if enableLog {
				fmt.Printf("[Worker %d] [batch %d]: construct DAG %d/%d, try entry next phase.\n",
					workerID, state.BatchID, completedMergeCount, totalMerges)
			}
		} else {
			// 最后一次合并，唤醒所有等待的
			// TODO: 唤醒之前需要先找到所有的弱连通分量
			// 🆕 日志：打印最终的连通分量结果
			finalDag := cdr.dagQueue[0]
			components := finalDag.GetConnectedComponents()

			//singleNodeCount := 0
			//multiNodeCount := 0
			//multiNodeTxCount := 0
			//for _, nodes := range components {
			//	if len(nodes) == 1 {
			//		singleNodeCount++
			//		state.CommittedTxs = append(state.CommittedTxs, nodes[0])
			//	} else {
			//		multiNodeCount++
			//		multiNodeTxCount += len(nodes)
			//		cdr.componentsQueue = append(cdr.componentsQueue, nodes)
			//	}
			//}
			//fmt.Printf("[Batch %d] 单节点(直接提交)=%d, 多节点连通分量=%d(共%d笔交易), 总计=%d\n",
			//	state.BatchID, singleNodeCount, multiNodeCount, multiNodeTxCount,
			//	singleNodeCount+multiNodeTxCount)

			// 初始化连通分量队列
			for _, nodes := range components {
				if len(nodes) == 1 {
					state.CommittedTxs = append(state.CommittedTxs, nodes[0])
				} else {
					cdr.componentsQueue = append(cdr.componentsQueue, nodes)
				}
			}
			cdr.totalComponents = len(cdr.componentsQueue)

			pe.timeOfConstructDAGPhase += time.Since(state.startTimeOfConstructDAGPhase)
			state.startTimeOfCommitMaximumValidationPhase = time.Now()

			cdr.done.Store(true)
			if enableLog {
				fmt.Printf("[Worker %d] [batch %d]: constrcut dag %d/%d completed, broadcasting to all busy waiting...\n",
					workerID, state.BatchID, completedMergeCount, totalMerges)

				//fmt.Printf("最终 DAG 连通分量结果: 总节点数=%d, 总边数=%d, 连通分量数=%d\n", len(finalDag.Nodes), countEdges(finalDag), len(components))
				//fmt.Printf("=======================================================================\n\n")
				//for root, nodes := range components {
				//	fmt.Printf("  连通分量 (root=%d): %v (大小=%d)\n", root, nodes, len(nodes))
				//	for _, node := range nodes {
				//		fmt.Println(finalDag.Nodes[node].ReadSet, finalDag.Nodes[node].WriteSet)
				//	}
				//	fmt.Println()
				//}
				//fmt.Printf("=======================================================================\n\n")
			}
		}
		return true // 可以先休息一会，休息完继续做牛马
	}
	return false // 需要继续做牛马
}

// 统计边数
func countEdges(dag *ConflictDAG) int {
	count := 0
	for _, neighbors := range dag.Edges {
		count += len(neighbors)
	}
	return count / 2 // 无向图，每条边被计算两次
}

// tryGetTaskAndActiveWorker 原子地获取 slot 并标记 worker 为活跃
func (cdr *constructDAGResult) tryGetTaskAndActiveWorker(workerID int) (int, bool) {
	if workerID >= 32 {
		panic("workerID must be < 32")
	}

	for {
		oldPacked := cdr.packedState.Load()
		oldIndex := int32(oldPacked & 0xFFFFFFFF)
		oldWorkers := uint32(oldPacked >> 32)
		if int(oldIndex) >= len(cdr.stateTables) {
			return -1, false
		}

		// 新状态：index+2，并设置 workerID 的位
		newIndex := oldIndex + 2
		newWorkers := oldWorkers | (1 << workerID) // 设置第 workerID 位为 1
		newPacked := uint64(newIndex) | (uint64(newWorkers) << 32)

		// 一个原子操作完成两个更新
		if cdr.packedState.CompareAndSwap(oldPacked, newPacked) {
			return int(oldIndex), true // 这里是从0开始返回的
		}
	}
}

func (cdr *constructDAGResult) CountActiveWorkersFast() int {
	packed := cdr.packedState.Load()
	workers := uint32(packed >> 32)
	return bits.OnesCount32(workers) // 硬件指令，超快！
}

func newConstructDAGResult(threadNumber int) *constructDAGResult {
	cdr := &constructDAGResult{
		stateTables:        make([]*StateTable, 0),
		dags:               make([]*ConflictDAG, threadNumber),
		dagQueue:           make([]*ConflictDAG, 0),
		componentsQueue:    make([][]int, 0),
		threadCommittedTxs: make([][]int, threadNumber),
		abortedTxs:         make([][]int, 0),
		threadAbortedTxs:   make([][][]int, threadNumber),
	}
	return cdr
}

// ConflictEdge 冲突边信息
type ConflictEdge struct {
	From    int    // 源交易ID
	To      int    // 目标交易ID
	Address string // 冲突的状态地址
	Type    string // 冲突类型："WR" 或 "WRW"
}

// ConflictDAG 冲突有向无环图
type ConflictDAG struct {
	Nodes       map[int]*ReadWriteSet    // 节点：交易ID -> 读写集
	Edges       map[int]map[int]struct{} // 【简化】边：nodeA -> {nodeB:  {}, nodeC: {}}，无向边
	Degree      map[int]int              // 度数（每个节点的邻接边数量）
	totalMerges int                      // 需要合并的总次数 (n-1)

	// 并查集，用于维护连通分量
	parent map[int]int // 并查集的父节点映射 parent[x] = x 的父节点
	rank   map[int]int // 并查集的秩映射 rank[x] = x 的秩（用于按秩合并优化
	//parent []int
	//rank   []int
}

//	func (dag *ConflictDAG) Find(x int) int {
//		// 初始化
//		if _, exists := dag.parent[x]; !exists {
//			dag.parent[x] = x
//			dag.rank[x] = 0
//		}
//		// 路径压缩
//		if dag.parent[x] != x {
//			dag.parent[x] = dag.Find(dag.parent[x]) // 路径压缩
//		}
//		return dag.parent[x]
//	}
func (dag *ConflictDAG) Find(x int) int {
	if _, exists := dag.parent[x]; !exists {
		dag.parent[x] = x
		dag.rank[x] = 0
		return x
	}
	// 第一遍：找到根
	root := x
	for dag.parent[root] != root {
		root = dag.parent[root]
	}
	// 第二遍：路径压缩
	for x != root {
		next := dag.parent[x]
		dag.parent[x] = root
		x = next
	}
	return root
}

// 并查集按秩合并
func (dag *ConflictDAG) Union(x, y int) {
	rootX := dag.Find(x)
	rootY := dag.Find(y)

	if rootX != rootY {
		// 按秩合并
		if dag.rank[rootX] < dag.rank[rootY] {
			dag.parent[rootX] = rootY
		} else if dag.rank[rootX] > dag.rank[rootY] {
			dag.parent[rootY] = rootX
		} else {
			dag.parent[rootY] = rootX
			dag.rank[rootX]++
		}
	}
}

// 获取所有连通分量
func (dag *ConflictDAG) GetConnectedComponents() map[int][]int {
	components := make(map[int][]int)
	for node := range dag.Nodes {
		root := dag.Find(node)
		components[root] = append(components[root], node)
	}
	return components
}

// StateTable 状态读写表
type StateTable struct {
	Address    string
	Operations []*Operation // 读写操作列表（按交易ID递增）
}

// Operation 读写操作
type Operation struct {
	TxID int
	//Type string // "r" for read, "w" for write
	Type byte // 'r' = 0, 'w' = 1
}

// PipelineEngine 流水线引擎
type PipelineEngine struct {
	levms      []*lvm.LEVM
	numThreads int

	// 工作线程控制
	stopChan      chan struct{}
	workerWg      sync.WaitGroup
	workerStaties []*WorkerStats

	// 批次管理（使用切片）
	janusTransactions []*janusTransaction
	batchStates       []*BatchState
	currentBatchID    atomic.Int32 // 当前批次索引
	currentBlockID    atomic.Int32

	timeOfExecuteCurrentBatchPhase     time.Duration
	timeOfExecuteCurrentBatchTail      time.Duration
	timeOfMergeStateTablePhase         time.Duration
	timeOfConstructDAGPhase            time.Duration
	timeOfCommitMaximumValidationPhase time.Duration
	timeOfReExecutePhase               time.Duration

	abortTxs []*janusTransaction

	// 完成通知
	completeChan chan int
}

const (
	WaitingPhase                 = iota // 等待任务
	ExecuteCurrentBatchPhase            // 执行当前批次的交易
	PreExecuteNextBatchPhase            // 预执行下一批的交易
	ConstructDAGPhase                   // 构建DAG
	CommitMaximumValidationPhase        // 最大可提交集
	ReExecutePhase                      // 重执行阶段
)

func (pe *PipelineEngine) tryEntryNextBatch(state *BatchState, workerID int, startTime *time.Time, timeOf *time.Duration) {
	pe.workerStaties[workerID].Phase = WaitingPhase
	ok := state.finished.CompareAndSwap(false, true)
	if ok {
		batchID := pe.currentBatchID.Add(1)
		*timeOf += time.Since(*startTime)
		if int(batchID) < len(pe.batchStates) {
			pe.batchStates[batchID].startTimeOfExecuteCurrentBatchPhase = time.Now()
		}
		//fmt.Printf("[ERROR] [Batch %d] CommittedTxs=%d != TotalTxs=%d, 丢失=%d笔\n",
		//	state.BatchID, len(state.CommittedTxs), state.TotalTxs,
		//	state.TotalTxs-len(state.CommittedTxs))
		committedTxsNum.Add(int32(len(state.CommittedTxs)))
		pe.completeBatch(state) // 完成该批次
	}
	if enableLog {
		fmt.Printf("[Worker %d] [batch %d] Finished batch %d, entry new batch %d\n",
			workerID, pe.workerStaties[workerID].currentBatchID, pe.workerStaties[workerID].currentBatchID, pe.currentBatchID.Load())
	}
}

func (pe *PipelineEngine) changeToNextBatch(state *BatchState, workerID int) bool {
	if state.finished.Load() {
		pe.workerStaties[workerID].Phase = WaitingPhase
		//if enableLog {
		//fmt.Printf("[WARNNING] [Worker %d] pipeline engine was entried next batch(%d), "+
		//	"this worker is entring waiting phase.\n", workerID, pe.currentBatchID.Load())
		//}
		return true
	}
	return false
}

type WorkerStats struct {
	currentBatchID int32
	CurrentBlockID int32
	workerID       int
	Phase          int
}

func NewWorkerStats(workerID int) *WorkerStats {
	return &WorkerStats{
		currentBatchID: -1,
		CurrentBlockID: -1,
		workerID:       workerID,
		Phase:          WaitingPhase,
	}
}
