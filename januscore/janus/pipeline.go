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
	WorkExec      WorkType = iota // 执行交易
	WorkValidate                  // 验证任务
	WorkReExecute                 // 重执行任务
)

// BatchState 批次状态
type BatchState struct {
	BatchID   int
	Batch     *Batch
	NextBatch *Batch

	// 交易队列
	LongTxChan    chan *types.Transaction
	ShortTxChan   chan *types.Transaction
	NextBatchChan chan *types.Transaction

	// 执行结果
	ExecResults   []*ReadWriteSet
	ExecResultsMu sync.Mutex
	ThreadRWSets  map[int][]*ReadWriteSet

	// 执行计数
	ExecCompleted int32
	TotalTxs      int
	ThreadsInNext int32

	// 验证状态
	ValidationStarted int32
	CommittedTxs      []*ReadWriteSet
	AbortedTxs        []*ReadWriteSet

	mu sync.Mutex
}

// PipelineEngine 流水线引擎
type PipelineEngine struct {
	levms      []*lvm.LEVM
	numThreads int

	// 8个工作线程共享的工作队列
	workQueue chan *WorkItem
	stopChan  chan struct{}
	workerWg  sync.WaitGroup

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

// workerThread 工作线程（8个线程处理所有任务）
func (pe *PipelineEngine) workerThread(workerID int) {
	defer pe.workerWg.Done()

	for {
		select {
		case work := <-pe.workQueue:
			if work == nil {
				return
			}
			pe.processWork(work, workerID)

		case <-pe.stopChan:
			return
		}
	}
}

// processWork 处理工作
func (pe *PipelineEngine) processWork(work *WorkItem, workerID int) {
	switch work.Type {
	case WorkExec:
		result := pe.executeTransaction(work.Data.(*ExecutionTask), workerID)
		work.ResultChan <- result

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
		BatchID:       batch.ID,
		Batch:         batch,
		NextBatch:     nextBatch,
		LongTxChan:    make(chan *types.Transaction, len(batch.LongTxs)),
		ShortTxChan:   make(chan *types.Transaction, len(batch.ShortTxs)),
		NextBatchChan: make(chan *types.Transaction, 1000),
		ThreadRWSets:  make(map[int][]*ReadWriteSet),
		ExecResults:   make([]*ReadWriteSet, 0),
		TotalTxs:      len(batch.AllTxs),
	}

	// 填充当前批次交易队列
	for _, tx := range batch.LongTxs {
		state.LongTxChan <- tx
	}
	close(state.LongTxChan)

	for _, tx := range batch.ShortTxs {
		state.ShortTxChan <- tx
	}
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

	// 启动所有线程执行该批次
	for i := 0; i < pe.numThreads; i++ {
		go pe.executeWorker(state, i)
	}
}

// executeWorker 执行工作逻辑（每个线程的执行流程）
func (pe *PipelineEngine) executeWorker(state *BatchState, workerID int) {
	for {
		executed := false

		// 1. 优先执行长交易
		select {
		case tx, ok := <-state.LongTxChan:
			if ok {
				pe.executeSingleTx(state, tx, workerID, true, false)
				executed = true
				continue
			}
		default:
		}

		// 2. 执行短交易
		select {
		case tx, ok := <-state.ShortTxChan:
			if ok {
				pe.executeSingleTx(state, tx, workerID, false, false)
				executed = true
				continue
			}
		default:
		}

		// 3. 当前批次没有交易了
		if !executed {
			// 检查当前批次是否全部完成
			completed := atomic.LoadInt32(&state.ExecCompleted)
			if int(completed) >= state.TotalTxs {
				// 当前批次已全部执行完成，结束该工作线程
				return
			}

			// 判断是执行下一批次还是验证
			threadsInNext := atomic.LoadInt32(&state.ThreadsInNext)

			if threadsInNext < int32(pe.numThreads/2) && state.NextBatch != nil {
				// 前 n/2 个线程：执行下一批次
				if atomic.CompareAndSwapInt32(&state.ThreadsInNext, threadsInNext, threadsInNext+1) {
					pe.executeNextBatch(state, workerID)
					return
				}
			}

			// 后 n/2 个线程：开始验证
			if atomic.CompareAndSwapInt32(&state.ValidationStarted, 0, 1) {
				// 第一个进入验证的线程负责启动验证
				go pe.waitAndValidate(state)
			}
			return
		}
	}
}

// executeSingleTx 执行单个交易
func (pe *PipelineEngine) executeSingleTx(state *BatchState, tx *types.Transaction, workerID int, isLong, isNext bool) {
	txID := int(atomic.LoadInt32(&state.ExecCompleted))

	task := &ExecutionTask{
		Tx:       tx,
		TxID:     txID,
		BatchID:  state.BatchID,
		IsLongTx: isLong,
	}

	// 提交到工作队列
	resultChan := make(chan interface{}, 1)
	pe.workQueue <- &WorkItem{
		Type:       WorkExec,
		BatchID:    state.BatchID,
		Data:       task,
		ResultChan: resultChan,
	}

	// 等待结果
	result := <-resultChan
	rwset := result.(*ReadWriteSet)

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

// executeNextBatch 执行下一批次交易（水位线间并发）
func (pe *PipelineEngine) executeNextBatch(state *BatchState, workerID int) {
	if state.NextBatch == nil {
		return
	}

	for {
		select {
		case tx, ok := <-state.NextBatchChan:
			if !ok {
				return
			}

			// 执行下一批次的交易
			task := &ExecutionTask{
				Tx:       tx,
				BatchID:  state.NextBatch.ID,
				IsLongTx: tx.TxType == 1,
			}

			resultChan := make(chan interface{}, 1)
			pe.workQueue <- &WorkItem{
				Type:       WorkExec,
				BatchID:    state.NextBatch.ID,
				Data:       task,
				ResultChan: resultChan,
			}

			result := <-resultChan
			rwset := result.(*ReadWriteSet)

			// Early Abort: 检查与当前批次的冲突
			if pe.hasConflictWithBatch(state, rwset) {
				rwset.Cost = 0 // 标记为中止
			}

			// 存储到下一批次（这里简化处理，实际应该存到下一批次的状态中）

		default:
			return
		}
	}
}

// waitAndValidate 等待执行完成并启动验证
func (pe *PipelineEngine) waitAndValidate(state *BatchState) {
	// 等待所有交易执行完成
	for {
		completed := atomic.LoadInt32(&state.ExecCompleted)
		if int(completed) >= state.TotalTxs {
			break
		}
	}

	// 开始验证
	pe.startValidation(state)
}

// startValidation 启动验证阶段
func (pe *PipelineEngine) startValidation(state *BatchState) {
	state.ExecResultsMu.Lock()
	rwsets := state.ExecResults
	threadRWSets := state.ThreadRWSets
	state.ExecResultsMu.Unlock()

	// 构建冲突图 DAG
	dag := pe.buildConflictDAG(rwsets, threadRWSets)
	subDAGs := pe.extractSubDAGs(dag)

	// 并发验证
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

	// 启动重执行
	if len(abortedTxs) > 0 {
		pe.startReExecution(state)
	} else {
		pe.completeBatch(state)
	}
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
	TxID     int
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
	tools.PanicError("SChain Tx Execute", err)

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
		TxID:     task.TxID,
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
	tools.PanicError("SChain Tx Execute", err)

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
