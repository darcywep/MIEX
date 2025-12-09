package janus

import (
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"
)

// WorkItem 工作项
type WorkItem struct {
	Type       WorkType
	BatchID    int
	Data       interface{}
	ResultChan chan interface{}
}

// WorkType 工作类型
type WorkType int

const (
	WorkExecCurrentBatch WorkType = iota // 执行当前批次交易
	WorkExecNextBatch                    // 执行下一批次交易
	WorkValidate                         // 验证任务
	WorkReExecute                        // 重执行任务
)

// BatchState 批次状态
type BatchState struct {
	BatchID   int
	Batch     *Batch
	NextBatch *Batch

	// 交易队列（带TxID信息）
	LongTxChan    chan *TxWithID
	ShortTxChan   chan *TxWithID
	NextBatchChan chan *types.Transaction

	// 执行结果
	ExecResults   []*ReadWriteSet
	ExecResultsMu sync.Mutex
	ThreadRWSets  map[int][]*ReadWriteSet

	// 执行计数
	ExecCompleted    int32
	TotalTxs         int
	ThreadsInNext    int32 // 有多少线程在执行下一批次
	ThreadsCompleted int32 // 有多少线程完成了当前批次

	// 验证状态
	ValidationStarted int32
	ValidationPairs   chan *ValidationPair // 验证配对通道
	CommittedTxs      []*ReadWriteSet
	AbortedTxs        []*ReadWriteSet

	mu sync.Mutex
}

// TxWithID 带原始ID的交易
type TxWithID struct {
	Tx   *types.Transaction
	TxID int // 区块链中的原始顺序ID
}

// ValidationPair 验证配对
type ValidationPair struct {
	WorkerID1 int
	WorkerID2 int
}

// PipelineEngine 流水线引擎
type PipelineEngine struct {
	levms      []*lvm.LEVM
	numThreads int

	// 8个工作线程共享的工作队列
	workQueue chan *WorkItem
	stopChan  chan struct{}
	workerWg  sync.WaitGroup

	// 当前正在处理的批次
	currentBatch   *BatchState
	currentBatchMu sync.RWMutex

	// 批次管理
	batches   map[int]*BatchState
	batchesMu sync.Mutex

	// 完成通知
	completeChan chan int
}

// NewPipelineEngine 创建流水线引擎
func NewPipelineEngine(levm *lvm.LEVM, numThreads int) *PipelineEngine {
	levms := make([]*lvm.LEVM, numThreads)
	for i := 0; i < numThreads; i++ {
		levms[i] = levm.Copy()
	}
	return &PipelineEngine{
		levms:        levms,
		numThreads:   numThreads,
		workQueue:    make(chan *WorkItem, 10000),
		stopChan:     make(chan struct{}),
		batches:      make(map[int]*BatchState),
		completeChan: make(chan int, 100),
	}
}

// Start 启动8个工作线程
func (pe *PipelineEngine) Start() {
	fmt.Printf("=== Starting %d worker threads ===\n", pe.numThreads)

	for i := 0; i < pe.numThreads; i++ {
		pe.workerWg.Add(1)
		go pe.workerThread(i)
	}
}

// Stop 停止引擎
func (pe *PipelineEngine) Stop() {
	close(pe.stopChan)
	pe.workerWg.Wait()
	close(pe.completeChan)
}

// workerThread 工作线程
func (pe *PipelineEngine) workerThread(workerID int) {
	defer pe.workerWg.Done()

	for {
		select {
		case <-pe.stopChan:
			return
		default:
		}

		// 获取当前批次
		pe.currentBatchMu.RLock()
		state := pe.currentBatch
		pe.currentBatchMu.RUnlock()

		if state == nil {
			// 没有当前批次，尝试从工作队列获取其他任务
			select {
			case work := <-pe.workQueue:
				if work != nil {
					pe.processWork(work, workerID)
				}
			case <-pe.stopChan:
				return
			default:
				continue
			}
			continue
		}

		// 尝试执行当前批次的交易
		executed := false

		// 1. 优先执行长交易
		select {
		case txWithID, ok := <-state.LongTxChan:
			if ok {
				pe.executeSingleTx(state, txWithID, workerID, true)
				executed = true
				continue
			}
		default:
		}

		// 2. 执行短交易
		if !executed {
			select {
			case txWithID, ok := <-state.ShortTxChan:
				if ok {
					pe.executeSingleTx(state, txWithID, workerID, false)
					executed = true
					continue
				}
			default:
			}
		}

		// 3. 当前批次没有交易了
		if !executed {
			// 标记该线程完成了当前批次
			completedThreads := atomic.AddInt32(&state.ThreadsCompleted, 1)

			fmt.Printf("[Worker %d] Completed current batch (thread %d/%d completed)\n",
				workerID, completedThreads, pe.numThreads)

			threadsInNext := atomic.LoadInt32(&state.ThreadsInNext)

			if threadsInNext < int32(pe.numThreads/2) && state.NextBatch != nil {
				// 前 n/2 个空闲线程：去执行下一批次
				if atomic.CompareAndSwapInt32(&state.ThreadsInNext, threadsInNext, threadsInNext+1) {
					fmt.Printf("[Worker %d] Executing next batch (thread %d/%d in next)\n",
						workerID, threadsInNext+1, pe.numThreads/2)
					pe.executeNextBatchTransactions(state, workerID)
					continue
				}
			}

			// 后 n/2 个空闲线程：参与验证
			// 等待配对进行DAG构建
			fmt.Printf("[Worker %d] Waiting for validation pairing\n", workerID)
			pe.waitForValidationPairing(state, workerID)

			// 配对验证完成后，继续处理其他任务
			select {
			case work := <-pe.workQueue:
				if work != nil {
					pe.processWork(work, workerID)
				}
			case <-pe.stopChan:
				return
			default:
				continue
			}
		}
	}
}

// executeSingleTx 执行单个交易
func (pe *PipelineEngine) executeSingleTx(state *BatchState, txWithID *TxWithID, workerID int, isLong bool) {
	task := &ExecutionTask{
		Tx:       txWithID.Tx,
		TxID:     txWithID.TxID, // 使用原始顺序ID
		BatchID:  state.BatchID,
		IsLongTx: isLong,
	}

	// 直接执行
	rwset := pe.executeTransaction(task, workerID)

	// 收集结果
	state.ExecResultsMu.Lock()
	state.ExecResults = append(state.ExecResults, rwset)
	if state.ThreadRWSets[workerID] == nil {
		state.ThreadRWSets[workerID] = make([]*ReadWriteSet, 0)
	}
	state.ThreadRWSets[workerID] = append(state.ThreadRWSets[workerID], rwset)
	state.ExecResultsMu.Unlock()

	// 增加完成计数
	atomic.AddInt32(&state.ExecCompleted, 1)
}

// executeNextBatchTransactions 执行下一批次的交易
func (pe *PipelineEngine) executeNextBatchTransactions(state *BatchState, workerID int) {
	if state.NextBatch == nil {
		return
	}

	for {
		select {
		case tx, ok := <-state.NextBatchChan:
			if !ok {
				return
			}

			task := &ExecutionTask{
				Tx:       tx,
				BatchID:  state.NextBatch.ID,
				IsLongTx: tx.TxType == janusConfig.ComputeTx,
			}

			rwset := pe.executeTransaction(task, workerID)

			// Early Abort: 检查与当前批次的冲突
			if pe.hasConflictWithBatch(state, rwset) {
				rwset.Cost = 0
				fmt.Printf("[Worker %d] Early abort tx from next batch\n", workerID)
			}

		default:
			return
		}
	}
}

// waitForValidationPairing 等待验证配对
// 根据文档：线程3完成后，与线程1配对构建子DAG；线程4完成后，与线程2配对构建子DAG
func (pe *PipelineEngine) waitForValidationPairing(state *BatchState, workerID int) {
	completedThreads := atomic.LoadInt32(&state.ThreadsCompleted)

	// 如果是前半部分完成的线程（去执行下一批次的），不参与验证配对
	if completedThreads <= int32(pe.numThreads/2) {
		return
	}

	// 后n/2个线程参与验证配对
	// 计算配对：第 (n/2 + 1) 个完成的线程 与 第1个完成的线程配对
	//         第 (n/2 + 2) 个完成的线程 与 第2个完成的线程配对
	//         ...
	pairIndex := completedThreads - int32(pe.numThreads/2) - 1

	// 等待所有交易执行完成
	for {
		execCompleted := atomic.LoadInt32(&state.ExecCompleted)
		if int(execCompleted) >= state.TotalTxs {
			break
		}
	}

	// 第一个进入的后n/2线程启动验证协调
	if atomic.CompareAndSwapInt32(&state.ValidationStarted, 0, 1) {
		fmt.Printf("[Worker %d] Starting validation coordination\n", workerID)
		go pe.coordinateValidation(state)
	}

	// 等待并执行配对的DAG构建
	select {
	case pair := <-state.ValidationPairs:
		if pair.WorkerID1 == workerID || pair.WorkerID2 == workerID {
			fmt.Printf("[Worker %d] Building sub-DAG with pair (%d, %d)\n",
				workerID, pair.WorkerID1, pair.WorkerID2)
			pe.buildPairDAG(state, pair.WorkerID1, pair.WorkerID2)
		}
	default:
	}
}

// coordinateValidation 协调验证过程
func (pe *PipelineEngine) coordinateValidation(state *BatchState) {
	fmt.Printf("[Validation] Coordinating validation for batch %d\n", state.BatchID)

	// 等待所有交易执行完成
	for {
		completed := atomic.LoadInt32(&state.ExecCompleted)
		if int(completed) >= state.TotalTxs {
			break
		}
	}

	state.ExecResultsMu.Lock()
	rwsets := state.ExecResults
	threadRWSets := state.ThreadRWSets
	state.ExecResultsMu.Unlock()

	// 构建完整的冲突图 DAG（合并所有线程的状态表）
	dag := pe.buildConflictDAG(rwsets, threadRWSets)
	subDAGs := pe.extractSubDAGs(dag)

	fmt.Printf("[Validation] Built complete DAG with %d sub-DAGs\n", len(subDAGs))

	// 将验证任务提交到工作队列
	resultChans := make([]chan interface{}, len(subDAGs))
	for i, subDAG := range subDAGs {
		resultChans[i] = make(chan interface{}, 1)

		pe.workQueue <- &WorkItem{
			Type:       WorkValidate,
			BatchID:    state.BatchID,
			Data:       &ValidationTask{SubDAG: subDAG},
			ResultChan: resultChans[i],
		}
	}

	// 收集验证结果
	committedTxs := make([]*ReadWriteSet, 0)
	for _, resultChan := range resultChans {
		result := <-resultChan
		committedTxs = append(committedTxs, result.([]*ReadWriteSet)...)
	}

	// 计算中止交易
	committedSet := make(map[int]bool)
	for _, rwset := range committedTxs {
		committedSet[rwset.TxID] = true
	}

	abortedTxs := make([]*ReadWriteSet, 0)
	for _, rwset := range rwsets {
		if !committedSet[rwset.TxID] {
			abortedTxs = append(abortedTxs, rwset)
		}
	}

	state.mu.Lock()
	state.CommittedTxs = committedTxs
	state.AbortedTxs = abortedTxs
	state.mu.Unlock()

	fmt.Printf("[Validation] Batch %d: %d committed, %d aborted\n",
		state.BatchID, len(committedTxs), len(abortedTxs))

	// 启动重执行
	if len(abortedTxs) > 0 {
		pe.startReExecution(state)
	} else {
		pe.completeBatch(state)
	}
}

// buildPairDAG 构建配对的子DAG（简化实现，实际在buildConflictDAG中完成）
func (pe *PipelineEngine) buildPairDAG(state *BatchState, workerID1, workerID2 int) {
	// 这里简化处理，实际的DAG构建在 buildConflictDAG 中通过合并状态表完成
	// 文档中的配对构建是为了并行加速，最终会合并成完整DAG
}

// processWork 处理工作队列中的任务
func (pe *PipelineEngine) processWork(work *WorkItem, workerID int) {
	switch work.Type {
	case WorkValidate:
		result := pe.validateDAG(work.Data.(*ValidationTask))
		work.ResultChan <- result

	case WorkReExecute:
		result := pe.reExecuteTransaction(work.Data.(*ReExecutionTask), workerID)
		work.ResultChan <- result
	}
}

// SubmitBatch 提交批次
func (pe *PipelineEngine) SubmitBatch(batch *Batch, nextBatch *Batch) {
	state := &BatchState{
		BatchID:         batch.ID,
		Batch:           batch,
		NextBatch:       nextBatch,
		LongTxChan:      make(chan *TxWithID, len(batch.LongTxs)),
		ShortTxChan:     make(chan *TxWithID, len(batch.ShortTxs)),
		NextBatchChan:   make(chan *types.Transaction, 1000),
		ValidationPairs: make(chan *ValidationPair, 10),
		ThreadRWSets:    make(map[int][]*ReadWriteSet),
		ExecResults:     make([]*ReadWriteSet, 0),
		TotalTxs:        len(batch.AllTxs),
	}

	// 为每笔交易分配原始顺序ID
	longTxIndex := 0
	shortTxIndex := 0

	for txID, tx := range batch.AllTxs {
		if tx.TxType == janusConfig.ComputeTx {
			// 长交易
			state.LongTxChan <- &TxWithID{Tx: tx, TxID: txID}
			longTxIndex++
		} else {
			// 短交易
			state.ShortTxChan <- &TxWithID{Tx: tx, TxID: txID}
			shortTxIndex++
		}
	}
	close(state.LongTxChan)
	close(state.ShortTxChan)

	// 填充下一批次交易队列
	if nextBatch != nil {
		for _, tx := range nextBatch.AllTxs {
			state.NextBatchChan <- tx
		}
		close(state.NextBatchChan)
	}

	pe.batchesMu.Lock()
	pe.batches[batch.ID] = state
	pe.batchesMu.Unlock()

	// 设置为当前批次
	pe.currentBatchMu.Lock()
	pe.currentBatch = state
	pe.currentBatchMu.Unlock()
}

// startReExecution 启动重执行阶段
func (pe *PipelineEngine) startReExecution(state *BatchState) {
	state.mu.Lock()
	abortedTxs := state.AbortedTxs
	state.mu.Unlock()

	groups := pe.partitionByConflict(abortedTxs)

	// 并发重执行
	resultChans := make([]chan interface{}, 0)
	for _, group := range groups {
		for _, rwset := range group {
			resultChan := make(chan interface{}, 1)
			resultChans = append(resultChans, resultChan)

			pe.workQueue <- &WorkItem{
				Type:       WorkReExecute,
				BatchID:    state.BatchID,
				Data:       &ReExecutionTask{OldRWSet: rwset},
				ResultChan: resultChan,
			}
		}
	}

	// 收集重执行结果
	reExecResults := make([]*ReadWriteSet, 0)
	for _, resultChan := range resultChans {
		result := <-resultChan
		reExecResults = append(reExecResults, result.(*ReadWriteSet))
	}

	state.mu.Lock()
	state.CommittedTxs = append(state.CommittedTxs, reExecResults...)
	state.AbortedTxs = []*ReadWriteSet{}
	state.mu.Unlock()

	pe.completeBatch(state)
}

// completeBatch 完成批次
func (pe *PipelineEngine) completeBatch(state *BatchState) {
	pe.currentBatchMu.Lock()
	if pe.currentBatch == state {
		pe.currentBatch = nil
	}
	pe.currentBatchMu.Unlock()

	pe.completeChan <- state.BatchID
}

// hasConflictWithBatch 检查与当前批次的冲突
func (pe *PipelineEngine) hasConflictWithBatch(state *BatchState, rwset *ReadWriteSet) bool {
	state.ExecResultsMu.Lock()
	defer state.ExecResultsMu.Unlock()

	for _, existRWSet := range state.ExecResults {
		if pe.hasConflict(rwset, existRWSet) {
			return true
		}
	}
	return false
}

// WaitForCompletion 等待所有批次完成
func (pe *PipelineEngine) WaitForCompletion(expectedBatches int) []*ValidationResult {
	results := make([]*ValidationResult, 0, expectedBatches)

	for i := 0; i < expectedBatches; i++ {
		batchID := <-pe.completeChan

		pe.batchesMu.Lock()
		state := pe.batches[batchID]
		pe.batchesMu.Unlock()

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

	return results
}

// ExecutionTask 执行任务
type ExecutionTask struct {
	Tx       *types.Transaction
	TxID     int // 区块链原始顺序ID
	BatchID  int
	IsLongTx bool
}

// ValidationTask 验证任务
type ValidationTask struct {
	SubDAG *ConflictDAG
}

// ReExecutionTask 重执行任务
type ReExecutionTask struct {
	OldRWSet *ReadWriteSet
}

// executeTransaction 执行交易
func (pe *PipelineEngine) executeTransaction(task *ExecutionTask, workerID int) *ReadWriteSet {
	readSet := make(map[string]struct{})
	writeSet := make(map[string]struct{})
	tx := task.Tx
	_, err := pe.levms[workerID].CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("Janus Tx Execute", err)

	if tx.TxType == janusConfig.IOTx {
		key1 := tx.From().String()
		key2 := tx.SmallBankTo.String()
		writeSet[key1] = struct{}{}
		writeSet[key2] = struct{}{}
		readSet[key1] = struct{}{}
		readSet[key2] = struct{}{}
	} else {
		key1 := tx.SmallBankTo.String()
		writeSet[key1] = struct{}{}
		readSet[key1] = struct{}{}
	}

	return &ReadWriteSet{
		TxID:     task.TxID, // 使用原始顺序ID
		Tx:       task.Tx,
		ReadSet:  readSet,
		WriteSet: writeSet,
		Cost:     tx.ExecutionCost,
		ThreadID: workerID,
	}
}

func (pe *PipelineEngine) validateDAG(task *ValidationTask) []*ReadWriteSet {
	return pe.solveSubDAG(task.SubDAG)
}

func (pe *PipelineEngine) reExecuteTransaction(task *ReExecutionTask, workerID int) *ReadWriteSet {
	oldRWSet := task.OldRWSet
	readSet := make(map[string]struct{})
	writeSet := make(map[string]struct{})
	tx := oldRWSet.Tx
	_, err := pe.levms[workerID].CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("Janus Tx Execute", err)

	if tx.TxType == janusConfig.IOTx {
		key1 := tx.From().String()
		key2 := tx.SmallBankTo.String()
		writeSet[key1] = struct{}{}
		writeSet[key2] = struct{}{}
		readSet[key1] = struct{}{}
		readSet[key2] = struct{}{}
	} else {
		key1 := tx.SmallBankTo.String()
		writeSet[key1] = struct{}{}
		readSet[key1] = struct{}{}
	}

	return &ReadWriteSet{
		TxID:     oldRWSet.TxID,
		Tx:       oldRWSet.Tx,
		ReadSet:  readSet,
		WriteSet: writeSet,
		Cost:     tx.ExecutionCost,
		ThreadID: workerID,
	}
}
