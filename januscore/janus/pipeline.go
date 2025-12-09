package janus

import (
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
)

// WorkItem 工作项
type WorkItem struct {
	Type       WorkType
	BatchID    int
	Priority   int // 优先级：越小越高（长交易=1, 短交易=2, 下一批次=3, 验证=4, 重执行=5）
	Data       interface{}
	ResultChan chan interface{}
}

// WorkType 工作类型
type WorkType int

const (
	WorkExecLong  WorkType = iota // 执行长交易
	WorkExecShort                 // 执行短交易
	WorkExecNext                  // 执行下一批次
	WorkValidate                  // 验证任务
	WorkReExecute                 // 重执行任务
)

// BatchState 批次状态
type BatchState struct {
	BatchID   int
	Batch     *Batch
	NextBatch *Batch

	// 执行结果
	ExecResults   []*ReadWriteSet
	ExecResultsMu sync.Mutex
	ThreadRWSets  map[int][]*ReadWriteSet

	// 执行计数
	ExecCompleted int32
	TotalTxs      int
	ThreadsInNext int32 // 有多少线程在执行下一批次

	// 任务提交状态
	LongTxSubmitted  int32
	ShortTxSubmitted int32
	NextTxSubmitted  int32

	// 验证状态
	ValidationStarted int32
	CommittedTxs      []*ReadWriteSet
	AbortedTxs        []*ReadWriteSet

	mu sync.Mutex
}

// TxWithID 带原始ID的交易
type TxWithID struct {
	Tx   *types.Transaction
	TxID int // 区块链中的原始顺序ID
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
// 统一从 workQueue 获取任务：
// - 执行长交易（优先级最高）
// - 执行短交易
// - 执行下一批次交易
// - 验证任务
// - 重执行任务
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
	case WorkExecLong, WorkExecShort:
		result := pe.executeTransaction(work.Data.(*ExecutionTask), workerID)

		// 收集结果
		pe.currentBatchMu.RLock()
		state := pe.currentBatch
		pe.currentBatchMu.RUnlock()

		if state != nil && state.BatchID == work.BatchID {
			state.ExecResultsMu.Lock()
			state.ExecResults = append(state.ExecResults, result)
			if state.ThreadRWSets[workerID] == nil {
				state.ThreadRWSets[workerID] = make([]*ReadWriteSet, 0)
			}
			state.ThreadRWSets[workerID] = append(state.ThreadRWSets[workerID], result)
			state.ExecResultsMu.Unlock()

			// 增加完成计数
			completed := atomic.AddInt32(&state.ExecCompleted, 1)

			// 检查是否完成当前批次
			if int(completed) == state.TotalTxs {
				// 当前批次执行完成，启动验证
				go pe.startValidation(state)
			}
		}

		work.ResultChan <- result

	case WorkExecNext:
		result := pe.executeTransaction(work.Data.(*ExecutionTask), workerID)

		// Early Abort 检查
		pe.currentBatchMu.RLock()
		state := pe.currentBatch
		pe.currentBatchMu.RUnlock()

		if state != nil && pe.hasConflictWithBatch(state, result) {
			result.Cost = 0
			fmt.Printf("[Worker %d] Early abort tx from next batch\n", workerID)
		}

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
		BatchID:      batch.ID,
		Batch:        batch,
		NextBatch:    nextBatch,
		ThreadRWSets: make(map[int][]*ReadWriteSet),
		ExecResults:  make([]*ReadWriteSet, 0),
		TotalTxs:     len(batch.AllTxs),
	}

	pe.batchesMu.Lock()
	pe.batches[batch.ID] = state
	pe.batchesMu.Unlock()

	// 设置为当前批次
	pe.currentBatchMu.Lock()
	pe.currentBatch = state
	pe.currentBatchMu.Unlock()

	// 启动任务提交协程
	go pe.submitBatchTasks(state)
}

// submitBatchTasks 提交批次任务
// 按优先级提交任务到工作队列：长交易 > 短交易
func (pe *PipelineEngine) submitBatchTasks(state *BatchState) {
	// 提交长交易（优先级1）
	for txID, tx := range state.Batch.AllTxs {
		if tx.TxType == janusConfig.ComputeTx {
			resultChan := make(chan interface{}, 1)

			pe.workQueue <- &WorkItem{
				Type:     WorkExecLong,
				BatchID:  state.BatchID,
				Priority: 1,
				Data: &ExecutionTask{
					Tx:       tx,
					TxID:     txID,
					BatchID:  state.BatchID,
					IsLongTx: true,
				},
				ResultChan: resultChan,
			}

			// 异步等待结果（不阻塞提交）
			go func() { <-resultChan }()

			atomic.AddInt32(&state.LongTxSubmitted, 1)
		}
	}

	// 提交短交易（优先级2）
	for txID, tx := range state.Batch.AllTxs {
		if tx.TxType == janusConfig.IOTx {
			resultChan := make(chan interface{}, 1)

			pe.workQueue <- &WorkItem{
				Type:     WorkExecShort,
				BatchID:  state.BatchID,
				Priority: 2,
				Data: &ExecutionTask{
					Tx:       tx,
					TxID:     txID,
					BatchID:  state.BatchID,
					IsLongTx: false,
				},
				ResultChan: resultChan,
			}

			go func() { <-resultChan }()

			atomic.AddInt32(&state.ShortTxSubmitted, 1)
		}
	}

	// 等待一段时间后，提交下一批次任务（水位线间并发）
	// 允许前 n/2 个线程执行下一批次
	if state.NextBatch != nil {
		go pe.submitNextBatchTasks(state)
	}
}

// submitNextBatchTasks 提交下一批次任务
func (pe *PipelineEngine) submitNextBatchTasks(state *BatchState) {
	// 等待当前批次大部分任务被提交
	for {
		submitted := atomic.LoadInt32(&state.LongTxSubmitted) + atomic.LoadInt32(&state.ShortTxSubmitted)
		if int(submitted) >= state.TotalTxs/2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 提交下一批次任务（限制数量，只给前 n/2 个线程）
	maxNextTasks := (pe.numThreads / 2) * 10 // 每个线程最多处理10笔

	for txID, tx := range state.NextBatch.AllTxs {
		if int(atomic.LoadInt32(&state.NextTxSubmitted)) >= maxNextTasks {
			break
		}

		resultChan := make(chan interface{}, 1)

		pe.workQueue <- &WorkItem{
			Type:     WorkExecNext,
			BatchID:  state.NextBatch.ID,
			Priority: 3,
			Data: &ExecutionTask{
				Tx:       tx,
				TxID:     txID,
				BatchID:  state.NextBatch.ID,
				IsLongTx: tx.TxType == janusConfig.ComputeTx,
			},
			ResultChan: resultChan,
		}

		go func() { <-resultChan }()

		atomic.AddInt32(&state.NextTxSubmitted, 1)
	}
}

// startValidation 启动验证阶段
func (pe *PipelineEngine) startValidation(state *BatchState) {
	// 确保只启动一次
	if !atomic.CompareAndSwapInt32(&state.ValidationStarted, 0, 1) {
		return
	}

	fmt.Printf("[Validation] Starting validation for batch %d\n", state.BatchID)

	state.ExecResultsMu.Lock()
	rwsets := state.ExecResults
	threadRWSets := state.ThreadRWSets
	state.ExecResultsMu.Unlock()

	// 构建冲突图 DAG
	dag := pe.buildConflictDAG(rwsets, threadRWSets)
	subDAGs := pe.extractSubDAGs(dag)

	fmt.Printf("[Validation] Built DAG with %d sub-DAGs\n", len(subDAGs))

	// 提交验证任务到工作队列（8个线程并发验证）
	resultChans := make([]chan interface{}, len(subDAGs))
	for i, subDAG := range subDAGs {
		resultChans[i] = make(chan interface{}, 1)

		pe.workQueue <- &WorkItem{
			Type:       WorkValidate,
			BatchID:    state.BatchID,
			Priority:   4,
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
		go pe.startReExecution(state)
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

	fmt.Printf("[ReExecution] Batch %d: %d groups\n", state.BatchID, len(groups))

	// 提交重执行任务到工作队列
	resultChans := make([]chan interface{}, 0)
	for _, group := range groups {
		for _, rwset := range group {
			resultChan := make(chan interface{}, 1)
			resultChans = append(resultChans, resultChan)

			pe.workQueue <- &WorkItem{
				Type:       WorkReExecute,
				BatchID:    state.BatchID,
				Priority:   5,
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
