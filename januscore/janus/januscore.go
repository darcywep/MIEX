package janus

import (
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"fmt"
	"sync"
	"time"
)

// Batch 交易批次
type Batch struct {
	Txs         []*types.Transaction
	BatchID     int
	WatermarkID int
}

// RWRecord 读写记录
type RWRecord struct {
	TxID      int
	OpType    string // "r" or "w"
	StateAddr string
}

// StateRWTable 状态读写表
type StateRWTable struct {
	Address string
	Records []RWRecord // 按交易ID递增排序
}

// DAGNode DAG节点
type DAGNode struct {
	TxID     int
	Tx       *types.Transaction
	Children map[int]bool // 指向的后续节点
	Parents  map[int]bool // 来自的前驱节点
	Cost     int          // 交易的计算成本
}

// DAG 有向无环图
type DAG struct {
	Nodes map[int]*DAGNode
	mu    sync.RWMutex
}

// ExecutionResult 执行结果
type ExecutionResult struct {
	TxID     int
	ReadSet  map[string][]byte
	WriteSet map[string][]byte
	Success  bool
	GasUsed  uint64
}

// JanusEngine 主执行引擎
type JanusEngine struct {
	levm         *lvm.LEVM
	numThreads   int
	alpha        float64 // 水位线参数
	lambda       int     // 计算型交易的成本权重
	batchQueue   chan *Batch
	currentBatch *Batch
	nextBatch    *Batch
	abortedTxs   []*types.Transaction
	committedTxs []*types.Transaction
}

// NewJanusEngine 创建新的执行引擎
func NewJanusEngine(levm *lvm.LEVM, numThreads int, alpha float64, lambda int) *JanusEngine {
	return &JanusEngine{
		levm:       levm,
		numThreads: numThreads,
		alpha:      alpha,
		lambda:     lambda,
		batchQueue: make(chan *Batch, 10),
	}
}

// Run 主执行流程
func Run(blockTxs []types.Transactions, levm *lvm.LEVM) float64 {
	fmt.Println("=== Run Janus SChain ===")

	// 初始化引擎
	numThreads := 8 // 可配置
	alpha := 1.0
	lambda := 100
	engine := NewJanusEngine(levm, numThreads, alpha, lambda)

	blockNum := janusConfig.TxNum / janusConfig.BlockSize
	start := time.Now()

	for i := 0; i < blockNum; i++ {
		txs := blockTxs[i]
		engine.ProcessBlock(txs)
	}

	elapsed := time.Since(start)
	txNumber := janusConfig.TxNum
	tps := float64(txNumber) / elapsed.Seconds()
	fmt.Printf("Janus TPS: %.2f\n", tps)
	return tps
}

// ProcessBlock 处理单个区块
func (e *JanusEngine) ProcessBlock(txs types.Transactions) {
	// 1. 批次生成
	batches := e.GenerateBatches(txs)
	fmt.Printf("Generated %d batches\n", len(batches))

	// 流水线并行处理批次
	for i := 0; i < len(batches); i++ {
		e.currentBatch = batches[i]
		if i+1 < len(batches) {
			e.nextBatch = batches[i+1]
		} else {
			e.nextBatch = nil
		}

		// 2. 分级试探执行
		dag, execResults := e.PrioritizedSpeculativeExecution(e.currentBatch)

		// 3. 最大提交验证
		committedTxIDs, abortedTxIDs := e.CommitMaximumValidation(dag)

		// 4. 重执行被中止的交易（与下一批次并发）
		if len(abortedTxIDs) > 0 {
			e.ReExecute(abortedTxIDs, e.currentBatch, execResults)
		}

		fmt.Printf("Batch %d: Committed %d, Aborted %d\n",
			i, len(committedTxIDs), len(abortedTxIDs))
	}
}

// GenerateBatches 1. 批次生成模块
func (e *JanusEngine) GenerateBatches(txs types.Transactions) []*Batch {
	batches := make([]*Batch, 0)

	// 统计长交易（计算型交易）的位置
	longTxPositions := make([]int, 0)
	for i, tx := range txs {
		if tx.TxType == janusConfig.ComputeTx {
			longTxPositions = append(longTxPositions, i)
		}
	}

	if len(longTxPositions) == 0 {
		// 没有长交易，所有交易作为一个批次
		return []*Batch{{Txs: txs, BatchID: 0, WatermarkID: 0}}
	}

	// 计算水位线间隔
	interval := int(float64(e.numThreads) * e.alpha)
	if interval == 0 {
		interval = 1
	}

	watermarks := make([]int, 0)
	for i := 0; i < len(longTxPositions); i += interval {
		watermarks = append(watermarks, longTxPositions[i])
	}

	// 根据水位线划分批次
	prevWatermark := -1
	for i, watermark := range watermarks {
		batch := &Batch{
			Txs:         txs[prevWatermark+1 : watermark+1],
			BatchID:     i,
			WatermarkID: watermark,
		}
		batches = append(batches, batch)
		prevWatermark = watermark
	}

	// 最后一个批次
	if prevWatermark < len(txs)-1 {
		batch := &Batch{
			Txs:         txs[prevWatermark+1:],
			BatchID:     len(batches),
			WatermarkID: len(txs) - 1,
		}
		batches = append(batches, batch)
	}

	return batches
}

// PrioritizedSpeculativeExecution 2. 分级试探执行
func (e *JanusEngine) PrioritizedSpeculativeExecution(batch *Batch) (*DAG, map[int]*ExecutionResult) {
	txs := batch.Txs
	numWorkers := e.numThreads

	// 分离长交易和短交易
	longTxs := make([]*types.Transaction, 0)
	shortTxs := make([]*types.Transaction, 0)
	txIDMap := make(map[*types.Transaction]int)

	for i, tx := range txs {
		txIDMap[tx] = i
		if tx.TxType == janusConfig.ComputeTx {
			longTxs = append(longTxs, tx)
		} else {
			shortTxs = append(shortTxs, tx)
		}
	}

	// 使用MVCC并发执行
	taskQueue := make(chan *types.Transaction, len(txs))

	// 优先放入长交易
	for _, tx := range longTxs {
		taskQueue <- tx
	}
	for _, tx := range shortTxs {
		taskQueue <- tx
	}
	close(taskQueue)

	// 每个线程的状态读写表
	threadRWTables := make([]map[string]*StateRWTable, numWorkers)
	execResults := make(map[int]*ExecutionResult)
	resultsLock := sync.Mutex{}

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for workerID := 0; workerID < numWorkers; workerID++ {
		threadRWTables[workerID] = make(map[string]*StateRWTable)

		go func(wid int) {
			defer wg.Done()

			for tx := range taskQueue {
				txID := txIDMap[tx]

				// 使用MVCC执行交易
				result := e.executeTxMVCC(tx, txID)

				// 记录读写集到本地表
				e.recordRWSet(threadRWTables[wid], txID, result)

				resultsLock.Lock()
				execResults[txID] = result
				resultsLock.Unlock()

				// 水位线间并发：如果当前批次完成且线程数允许，开始执行下一批次
				if e.shouldExecuteNextBatch(wid, numWorkers) {
					e.executeNextBatchTx(wid)
				}
			}
		}(workerID)
	}

	wg.Wait()

	// 合并DAG
	dag := e.mergeDAGs(threadRWTables, txs, txIDMap)

	return dag, execResults
}

// executeTxMVCC 使用MVCC执行单个交易
func (e *JanusEngine) executeTxMVCC(tx *types.Transaction, txID int) *ExecutionResult {
	result := &ExecutionResult{
		TxID:     txID,
		ReadSet:  make(map[string][]byte),
		WriteSet: make(map[string][]byte),
		Success:  true,
	}

	// 这里应该调用实际的EVM执行
	// 简化示例：模拟执行并收集读写集
	// result.GasUsed = e.levm.Execute(tx, result.ReadSet, result.WriteSet)

	return result
}

// recordRWSet 记录读写集到线程本地表
func (e *JanusEngine) recordRWSet(rwTables map[string]*StateRWTable, txID int, result *ExecutionResult) {
	// 记录读集
	for addr := range result.ReadSet {
		if _, exists := rwTables[addr]; !exists {
			rwTables[addr] = &StateRWTable{
				Address: addr,
				Records: make([]RWRecord, 0),
			}
		}
		rwTables[addr].Records = append(rwTables[addr].Records, RWRecord{
			TxID:      txID,
			OpType:    "r",
			StateAddr: addr,
		})
	}

	// 记录写集
	for addr := range result.WriteSet {
		if _, exists := rwTables[addr]; !exists {
			rwTables[addr] = &StateRWTable{
				Address: addr,
				Records: make([]RWRecord, 0),
			}
		}
		rwTables[addr].Records = append(rwTables[addr].Records, RWRecord{
			TxID:      txID,
			OpType:    "w",
			StateAddr: addr,
		})
	}
}

// mergeDAGs 合并多个线程的DAG
func (e *JanusEngine) mergeDAGs(threadRWTables []map[string]*StateRWTable, txs []*types.Transaction, txIDMap map[*types.Transaction]int) *DAG {
	dag := &DAG{
		Nodes: make(map[int]*DAGNode),
	}

	// 初始化所有节点
	for _, tx := range txs {
		txID := txIDMap[tx]
		cost := 1
		if tx.TxType == janusConfig.ComputeTx {
			cost = e.lambda
		}
		dag.Nodes[txID] = &DAGNode{
			TxID:     txID,
			Tx:       tx,
			Children: make(map[int]bool),
			Parents:  make(map[int]bool),
			Cost:     cost,
		}
	}

	// 收集所有状态地址
	allAddrs := make(map[string]bool)
	for _, rwTable := range threadRWTables {
		for addr := range rwTable {
			allAddrs[addr] = true
		}
	}

	// 对每个状态地址，合并不同线程的读写表
	for addr := range allAddrs {
		allRecords := make([]RWRecord, 0)

		// 收集所有线程的记录
		for _, rwTable := range threadRWTables {
			if table, exists := rwTable[addr]; exists {
				allRecords = append(allRecords, table.Records...)
			}
		}

		// 按交易ID排序（类似合并有序链表）
		sortedRecords := mergeSortRecords(allRecords)

		// 根据读写依赖构建DAG边
		e.buildDAGEdges(dag, sortedRecords)
	}

	return dag
}

// mergeSortRecords 合并排序记录
func mergeSortRecords(records []RWRecord) []RWRecord {
	if len(records) <= 1 {
		return records
	}

	// 简单插入排序（实际应该用更高效的算法）
	for i := 1; i < len(records); i++ {
		key := records[i]
		j := i - 1
		for j >= 0 && records[j].TxID > key.TxID {
			records[j+1] = records[j]
			j--
		}
		records[j+1] = key
	}
	return records
}

// buildDAGEdges 根据读写记录构建DAG边
func (e *JanusEngine) buildDAGEdges(dag *DAG, records []RWRecord) {
	lastWriter := -1

	for i, record := range records {
		if record.OpType == "w" {
			// 写操作：从上一个写者指向当前写者
			if lastWriter != -1 {
				dag.Nodes[lastWriter].Children[record.TxID] = true
				dag.Nodes[record.TxID].Parents[lastWriter] = true
			}

			// 从所有在两个写之间的读者指向当前写者
			for j := i - 1; j >= 0; j-- {
				if records[j].OpType == "r" && (lastWriter == -1 || records[j].TxID > lastWriter) {
					dag.Nodes[records[j].TxID].Children[record.TxID] = true
					dag.Nodes[record.TxID].Parents[records[j].TxID] = true
				}
				if records[j].OpType == "w" {
					break
				}
			}

			lastWriter = record.TxID
		} else {
			// 读操作：如果之前有写者，建立依赖
			if lastWriter != -1 {
				dag.Nodes[lastWriter].Children[record.TxID] = true
				dag.Nodes[record.TxID].Parents[lastWriter] = true
			}
		}
	}
}

// CommitMaximumValidation 3. 最大提交验证
func (e *JanusEngine) CommitMaximumValidation(dag *DAG) ([]int, []int) {
	nodes := make([]*DAGNode, 0, len(dag.Nodes))
	for _, node := range dag.Nodes {
		nodes = append(nodes, node)
	}

	// 按交易ID排序
	// 这里简化处理，实际应该按拓扑序

	// 动态规划求解最大工作量子集
	n := len(nodes)
	dp := make([]int, n+1)
	selected := make([]bool, n)

	// 从后向前计算
	for i := n - 1; i >= 0; i-- {
		node := nodes[i]

		// 找到下一个无冲突的交易
		nextNonConflict := n
		for j := i + 1; j < n; j++ {
			hasConflict := false
			// 检查是否有冲突（通过DAG边判断）
			if _, exists := node.Children[nodes[j].TxID]; exists {
				hasConflict = true
			}
			if !hasConflict {
				nextNonConflict = j
				break
			}
		}

		// 不提交i 或 提交i
		notCommit := dp[i+1]
		commit := node.Cost + dp[nextNonConflict]

		if commit > notCommit {
			dp[i] = commit
			selected[i] = true
		} else {
			dp[i] = notCommit
			selected[i] = false
		}
	}

	// 收集提交和中止的交易
	committedTxIDs := make([]int, 0)
	abortedTxIDs := make([]int, 0)

	for i, node := range nodes {
		if selected[i] {
			committedTxIDs = append(committedTxIDs, node.TxID)
		} else {
			abortedTxIDs = append(abortedTxIDs, node.TxID)
		}
	}

	return committedTxIDs, abortedTxIDs
}

// ReExecute 4. 重执行中止的交易
func (e *JanusEngine) ReExecute(abortedTxIDs []int, batch *Batch, prevResults map[int]*ExecutionResult) {
	// 根据读写集冲突将交易分配给线程
	conflictGroups := e.groupByConflict(abortedTxIDs, prevResults)

	var wg sync.WaitGroup
	for groupID, txIDs := range conflictGroups {
		wg.Add(1)
		go func(gid int, ids []int) {
			defer wg.Done()
			for _, txID := range ids {
				tx := batch.Txs[txID]
				// 重新执行
				result := e.executeTxMVCC(tx, txID)

				// 检查与下一批次的冲突（early abort）
				if e.nextBatch != nil && e.hasConflictWithNextBatch(result) {
					// 提前中止，放入下一批次
					continue
				}

				// 验证通过，可以提交
				e.committedTxs = append(e.committedTxs, tx)
			}
		}(groupID, txIDs)
	}
	wg.Wait()
}

// groupByConflict 根据冲突分组
func (e *JanusEngine) groupByConflict(txIDs []int, results map[int]*ExecutionResult) map[int][]int {
	groups := make(map[int][]int)
	// 简化实现：使用状态地址哈希分组
	for _, txID := range txIDs {
		result := results[txID]
		groupID := 0
		for addr := range result.WriteSet {
			groupID = int(addr[0]) % e.numThreads
			break
		}
		groups[groupID] = append(groups[groupID], txID)
	}
	return groups
}

// shouldExecuteNextBatch 5. 水位线间并发：判断是否应该执行下一批次
func (e *JanusEngine) shouldExecuteNextBatch(workerID, totalWorkers int) bool {
	// 保留一半线程用于验证
	return workerID < totalWorkers/2 && e.nextBatch != nil
}

// executeNextBatchTx 执行下一批次的交易
func (e *JanusEngine) executeNextBatchTx(workerID int) {
	// 实现与下一批次的并发执行
	// 需要检测冲突并early abort
}

// hasConflictWithNextBatch 检查与下一批次是否有冲突
func (e *JanusEngine) hasConflictWithNextBatch(result *ExecutionResult) bool {
	// 简化实现
	return false
}
