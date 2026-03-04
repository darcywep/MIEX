package harmony

import (
	"Janus/baselines/common"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/tools"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
)

type Harmony struct {
	levm             *lvm.LEVM
	Statistics       *common.Statistics
	blocks           []*common.Block
	table            *HarmonyTable
	lockTable        *HarmonyLockTable
	enableInterBlock bool
	numThreads       int
	confirmExit      atomic.Int32
	stopFlag         atomic.Bool
	barrier          *HarmonyBarrier // 使用WaitGroup模拟barrier，或者使用其他同步机制
	counter          atomic.Int32
	workers          []*HarmonyExecutor
}

func NewHarmony(
	blocks []*common.Block,
	statistics *common.Statistics,
	numThreads int,
	tablePartitions int,
	enableInterBlock bool,
) *Harmony {
	barrier := NewHarmonyBarrier(numThreads, func() {
		//fmt.Println("batch complete")
	})

	harmony := &Harmony{
		blocks:           blocks,
		Statistics:       statistics,
		barrier:          barrier,
		table:            NewHarmonyTable(tablePartitions),
		lockTable:        NewHarmonyLockTable(tablePartitions),
		enableInterBlock: enableInterBlock,
		numThreads:       numThreads,
		workers:          make([]*HarmonyExecutor, numThreads),
	}

	return harmony
}

type HarmonyBarrier struct {
	wg           sync.WaitGroup
	onCompletion func()
	parties      int
	mu           sync.Mutex
	current      int
	cond         *sync.Cond
}

func NewHarmonyBarrier(parties int, completion func()) *HarmonyBarrier {
	b := &HarmonyBarrier{
		parties:      parties,
		onCompletion: completion,
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *HarmonyBarrier) ArriveAndWait() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.current++

	if b.current == b.parties {
		// 最后一个到达的goroutine
		if b.onCompletion != nil {
			b.onCompletion()
		}
		b.current = 0
		b.cond.Broadcast() // 唤醒所有等待的goroutine
	} else {
		b.cond.Wait() // 等待其他goroutine
	}
}

type HarmonyExecutor struct {
	statistics       *common.Statistics
	batchTxs         [][]*HarmonyTransaction
	table            *HarmonyTable
	lockTable        *HarmonyLockTable
	enableInterBlock bool
	numThreads       int
	confirmExit      *atomic.Int32
	stopFlag         *atomic.Bool
	barrier          *HarmonyBarrier
	counter          *atomic.Int32
	workerID         uint32
	batchIdx         uint32
	levm             *lvm.LEVM
}

func NewHarmonyExecutor(harmony *Harmony, workerID uint32, batchTxs [][]*HarmonyTransaction, levm *lvm.LEVM) *HarmonyExecutor {

	return &HarmonyExecutor{
		batchTxs:         batchTxs,
		statistics:       harmony.Statistics,
		table:            harmony.table,
		lockTable:        harmony.lockTable,
		enableInterBlock: harmony.enableInterBlock,
		stopFlag:         &harmony.stopFlag,
		barrier:          harmony.barrier,
		counter:          &harmony.counter,
		numThreads:       harmony.numThreads,
		confirmExit:      &harmony.confirmExit,
		workerID:         workerID,
		batchIdx:         0,
		levm:             levm,
	}
}

func (h *Harmony) Start(levm *lvm.LEVM) {
	fmt.Println("harmony ready to start...")

	// split blocks into batches
	batches := make([][][]*HarmonyTransaction, 0, len(h.blocks))

	for i, block := range h.blocks {
		txs := block.GetTxs()
		txPerThread := 1
		index := 0

		// create batch for current block
		batch := make([][]*HarmonyTransaction, h.numThreads)
		batchID := i + 1

		//// Step 1: 生成地址
		//addresses := tools.GenerateAddresses(1, len(txs))
		//fmt.Printf("生成地址数量: %d\n", len(addresses))
		//
		//// Step 2: 生成交易（Zipf 控制冲突率）
		//ethTxs := tools.GenerateSmallBankTxs(addresses, len(txs)/2, len(txs)/2,
		//	janusConfig.FibonacciN, janusConfig.RecursiveCalculateFibonacci, janusConfig.Skew)
		//fmt.Printf("生成交易数量: %d\n", len(ethTxs)) // 生成以太坊交易

		// get all batches of one block
		for j := 0; j < len(txs); j += txPerThread {
			batchIdx := index % h.numThreads

			for k := 0; k < txPerThread && j+k < len(txs); k++ {
				tx := txs[j+k]
				txID := tx.Txid
				txInner := tx
				batch[batchIdx] = append(batch[batchIdx], NewHarmonyTransaction(txInner, txID, uint32(batchID)))
			}
			index++
		}

		// store block batch
		batches = append(batches, batch)
		h.Statistics.JournalBlock()
	}

	var wg sync.WaitGroup

	h.levm = levm.Copy()

	for i := 0; i < h.numThreads; i++ {
		// create thread batches for current worker
		threadBatches := make([][]*HarmonyTransaction, 0, len(h.blocks))
		for j := 0; j < len(h.blocks); j++ {
			//fmt.Printf("i = %d, j=%d \n", i, j)
			threadBatches = append(threadBatches, batches[j][i])
		}

		wg.Add(1)
		worker := NewHarmonyExecutor(h, uint32(i), threadBatches, levm.Copy())
		h.workers[i] = worker

		runtime.LockOSThread()

		go func(workerID int, executor *HarmonyExecutor) {
			defer wg.Done()
			executor.Run()
		}(i, worker)

		//	// Pin thread (equivalent to C++ ThreadPool::PinRoundRobin)
		//	// Note: Go doesn't have direct thread pinning, but you can use runtime.LockOSThread()
		// if needed, though it's usually not necessary in Go
	}
	wg.Wait()

	for _, worker := range h.workers {
		worker.levm.AllDB().StateDB.FlushDirtyToNewStateDB(levm.AllDB().StateDB)
	}
	//h.levm.CommitStateChange()
}

// Run 执行交易
func (e *HarmonyExecutor) Run() {

	//fmt.Println("harmony executor is running...")

	if e.enableInterBlock {
		fmt.Printf("worker %d size of batchTxs %d", e.workerID, len(e.batchTxs))
		// 流式处理区块
		e.barrier.ArriveAndWait()
		e.InterBlockExecute(e.NextBatch())
	} else {
		//逐个区块处理
		for _, batch := range e.batchTxs {
			// stage 1: execute
			_stop := e.confirmExit.Load() == int32(e.numThreads)
			e.barrier.ArriveAndWait()
			if _stop {
				return
			}
			if e.stopFlag.Load() {
				addr := e.confirmExit.Load()
				atomic.CompareAndSwapInt32(&addr, int32(e.workerID), int32(e.workerID+1))
			}
			fmt.Printf("worker %d executing", e.workerID)

			for i := range batch {
				tx := &batch[i]
				// 记录开始时间
				(*tx).StartTime = time.Now()
				// 执行交易并处理读写依赖
				e.Execute(*tx)
				e.statistics.JournalExecute()
				//e.statistics.JournalOverheads(tx.CountOverheads())
			}

			// stage 2: verify + commit
			e.barrier.ArriveAndWait()

			//fmt.Printf("worker %d verifying", e.workerID)
			var beginTime time.Time
			if e.workerID == 0 {
				beginTime = time.Now()
			}

			for i := range batch {
				tx := &batch[i]
				e.Verify(*tx)
				if (*tx).FlagConflict {
					e.PrepareLockTable(*tx)
				} else {
					e.Commit(*tx)
					latency := time.Since((*tx).StartTime).Microseconds()
					e.statistics.JournalCommit(uint32(latency))
				}
			}

			//// stage 3: fallback
			e.barrier.ArriveAndWait()
			//if e.workerID == 0 {
			//	phaseTime := time.Since(beginTime).Microseconds()
			//	e.statistics.JournalRollbackExecution(uint32(phaseTime))
			//}

			fmt.Printf("worker %d fallbacking \n", e.workerID)

			//if e.workerID == 0 {
			//	beginTime = time.Now()
			//}
			for i := range batch {
				tx := &batch[i]
				if (*tx).FlagConflict {
					e.Fallback(*tx)
					e.statistics.JournalExecute()
					latency := time.Since((*tx).StartTime).Microseconds()
					e.statistics.JournalCommit(uint32(latency))
					//e.statistics.JournalRollback(tx.CountOverheads())
				}
			}

			// stage 4: clean up
			e.barrier.ArriveAndWait()
			if e.workerID == 0 {
				//phaseTime := time.Since(beginTime).Microseconds()
				//e.statistics.JournalReExecution(phaseTime)
			}
			fmt.Printf("worker %d cleaning up \n", e.workerID)

			for i := range batch {
				tx := &batch[i]
				e.CleanLockTable(*tx)
			}

			elapsed := time.Since(beginTime)

			fmt.Printf("CommitCount= %d \n", e.statistics.CommitCount.Load())
			fmt.Printf("交易实际被执行总次数 %d \n", e.statistics.ExecCount.Load())
			fmt.Printf("交易处理吞吐(TPS)= %f \n", float64(e.statistics.CommitCount.Load())/(elapsed.Seconds()))
		}
	}
}

// NextBatch 获取执行器的下一批交易
func (e *HarmonyExecutor) NextBatch() []*HarmonyTransaction {
	if e.batchIdx >= uint32(len(e.batchTxs)) {
		fmt.Printf("worker %d no more batch", e.workerID)
		return nil
	}
	batch := e.batchTxs[e.batchIdx]
	e.batchIdx++
	return batch
}

// InterBlockExecute 在跨块模式下执行交易
// batch 交易批次
func (e *HarmonyExecutor) InterBlockExecute(batch []*HarmonyTransaction) {
	// stage 1: execute
	_stop := e.confirmExit.Load() == int32(e.numThreads)
	if _stop {
		return
	}

	if e.stopFlag.Load() != false {
		for {
			old := e.confirmExit.Load()
			if atomic.CompareAndSwapInt32(&old, old, old+1) {
				break
			}
		}
	}
	//fmt.Printf("worker %d executing batch %d size %d \n", e.workerID, e.batchIdx, len(batch))

	for i := range batch {
		tx := &batch[i]
		// 执行交易并处理读写依赖
		(*tx).StartTime = time.Now()
		e.Execute(*tx) // 执行
		e.statistics.JournalExecute()
		//e.statistics.JournalOverheads(tx.CountOverheads())
	}

	//fmt.Printf("至今被执行的交易数目 %d \n", e.statistics.ExecCount.Load())

	//// stage 2: verify + commit
	e.barrier.ArriveAndWait()

	//fmt.Printf("worker %d verifying batch %d \n", e.workerID, e.batchIdx)
	//var beginTime time.Time
	//if e.workerID == 0 {
	//	beginTime = time.Now()
	//}

	conflictNum := 0

	for i := range batch {
		tx := &batch[i]
		e.Verify(*tx)

		if (*tx).FlagConflict {
			conflictNum++
		}

		if (*tx).FlagConflict {
			e.PrepareLockTable(*tx)
		} else {
			e.Commit(*tx)
			latency := time.Since((*tx).StartTime).Microseconds()
			e.statistics.JournalCommit(uint32(latency))
		}
	}
	//fmt.Printf("冲突的交易数目 %d \n", conflictNum)

	// stage 3: fallback
	e.barrier.ArriveAndWait()
	//if e.workerID == 0 {
	//	phaseTime := time.Since(beginTime).Microseconds()
	//	e.statistics.JournalRollbackExecution(uint32(phaseTime))
	//}
	//fmt.Printf("worker %d fallbacking batch %d \n", e.workerID, e.batchIdx)

	//beginTime = time.Now()
	for i := range batch {
		tx := &batch[i]
		if (*tx).FlagConflict {
			e.Fallback(*tx)
			e.statistics.JournalExecute()
			latency := time.Since((*tx).StartTime).Microseconds()
			e.statistics.JournalCommit(uint32(latency))
			//e.statistics.JournalRollback(tx.CountOverheads())
		}
	}

	//fmt.Printf("至今被执行的交易数目 %d \n", e.statistics.ExecCount.Load())

	// stage 4: 流式执行下一个区块
	if e.batchIdx < uint32(len(e.batchTxs)) {
		e.counter.Add(1)

		if e.counter.Load() == int32(e.numThreads) {
			//phaseTime := time.Since(beginTime).Microseconds()
			//e.statistics.JournalReExecution(phaseTime)
			e.counter.Store(0)
		}
		//fmt.Printf("worker %d streamly next block \n", e.workerID)
		e.InterBlockExecute(e.NextBatch())
	} else {
		// stage 5: clean up
		e.barrier.ArriveAndWait()
		if e.workerID == 0 {
			//phaseTime := time.Since(beginTime).Microseconds()
			//e.statistics.JournalReExecution(phaseTime)
		}
		fmt.Printf("worker %d cleaning up batch %d \n", e.workerID, e.batchIdx)
		for i := range batch {
			tx := &batch[i]
			e.CleanLockTable(*tx)
		}
	}
}

// / @brief execute a transaction and journal write operations locally
func (executor *HarmonyExecutor) Execute(tx *HarmonyTransaction) {
	//tools.CatStorageState = true

	_, err := executor.levm.CallContract(*tx.Tx.EthTx.From(), *tx.Tx.EthTx.To(), tx.Tx.EthTx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("Transaction Execute", err)

	if tx.Tx.EthTx.TxType == janusConfig.IOTx {

		key1 := tx.Tx.EthTx.From().String()
		key2 := tx.Tx.EthTx.SmallBankTo.String()
		tx.Tx.Vertex.WriteKeys[key1] = "value"
		tx.Tx.Vertex.WriteKeys[key2] = "value"

		tx.Tx.Vertex.ReadKeys[key2] = "value"
		tx.Tx.Vertex.ReadKeys[key2] = "value"

		//tx.WriteKeys = append(tx.WriteKeys, tx.From().String())
		//tx.WriteKeys = append(tx.WriteKeys, tx.SmallBankTo.String())
	} else {
		//tx.WriteKeys = append(tx.ReadKeys, tx.SmallBankTo.String())
		key1 := tx.Tx.EthTx.SmallBankTo.String()
		tx.Tx.Vertex.WriteKeys[key1] = "value"
		tx.Tx.Vertex.ReadKeys[key1] = "value"
	}

	readSet := make(map[string]bool)
	writeSet := make(map[string]bool)

	for key, _ := range tx.Tx.Vertex.ReadKeys { // 填充读集
		readSet[key] = true
	}
	executor.ExecutorGetStorage(tx, readSet)

	for key, _ := range tx.Tx.Vertex.WriteKeys { // 填充写集
		writeSet[key] = true
	}
	executor.ExecutorSetStorage(tx, writeSet, "value")
}

// / @brief verify transaction by checking min_out and max_in
func (executor *HarmonyExecutor) Verify(tx *HarmonyTransaction) {

	if executor.enableInterBlock {
		// when inter block is enabled, then
		// if tx.min_out < tx.id and tx.min_out <= tx.max_in, then conflict
		//   if tx.batch_id = tx.in_batch_id, then abort
		// meanwhile, if tx.min_out < tx.id and tx.out_batch_id < tx.batch_id, then abort
		if tx.MinOut < tx.ID && tx.MinOut <= tx.MaxIn && tx.BatchID == tx.InBatchID {
			tx.FlagConflict = true
		} else if tx.MinOut < tx.ID && tx.OutBatchID < tx.BatchID {
			tx.FlagConflict = true
		}
	} else {
		// when inter block is disabled, then
		// if tx.min_out < tx.id and tx.min_out <= tx.max_in, then conflict
		if tx.MinOut < tx.ID && tx.MinOut <= tx.MaxIn {
			tx.FlagConflict = true
		}
	}
	if tx.FlagConflict == true {
		//fmt.Printf("交易 %d 中止 \n", tx.ID)
	} else {
		//fmt.Printf("交易 %d 可以提交 \n", tx.ID)
	}
}

// / @brief commit written values into table
func (executor *HarmonyExecutor) Commit(tx *HarmonyTransaction) {
	for key, value := range tx.LocalPut {
		executor.table.table.Put(key, func(entry *HarmonyEntry) {
			(*entry).Value = value
		})
	}
	// atomic.AddUint64(&executor.counter, 1)
	//fmt.Printf("tx %d committed", tx.ID)
}

// / @brief put transaction id (local id) into table
func (executor *HarmonyExecutor) PrepareLockTable(tx *HarmonyTransaction) {
	for key := range tx.LocalGet {
		executor.lockTable.table.Put(key, func(entry *HarmonyLockEntry) {
			(*entry).DepsGet = append((*entry).DepsGet, tx)
		})
	}
	for key := range tx.LocalPut {
		executor.lockTable.table.Put(key, func(entry *HarmonyLockEntry) {
			(*entry).DepsPut = append((*entry).DepsPut, tx)
		})
	}
}

// / @brief fallback execution without constant
func (executor *HarmonyExecutor) Fallback(tx *HarmonyTransaction) {

	//// get the latest dependency and wait on it
	var should_wait *HarmonyTransaction = nil
	// 定义条件函数替代宏
	cond := func(_tx *HarmonyTransaction) bool {
		return _tx.ID < tx.ID && (should_wait == nil || _tx.ID > should_wait.ID)
	}

	for key := range tx.LocalPut {
		executor.lockTable.table.Get(key, func(entry HarmonyLockEntry) {
			for _, _tx := range entry.DepsGet {
				if cond(_tx) {
					should_wait = _tx
				}
			}
			for _, _tx := range entry.DepsPut {
				if cond(_tx) {
					should_wait = _tx
				}
			}
		})
	}
	for key := range tx.LocalGet {
		executor.lockTable.table.Get(key, func(entry HarmonyLockEntry) {
			for _, _tx := range entry.DepsPut {
				if cond(_tx) {
					should_wait = _tx
				}
			}
		})
	}

	// 等待依赖事务提交
	if should_wait != nil {
		for !should_wait.Committed.Load() {
			// 等待，可以添加一些退避策略
			runtime.Gosched()
		}
	}

	readSet := make(map[string]bool)
	for key := range tx.LocalGet {
		readSet[key] = true
	}

	writeSet := make(map[string]bool)
	for key := range tx.LocalPut {
		writeSet[key] = true
	}

	executor.ExecutorGetStorage2(tx, readSet)
	executor.ExecutorSetStorage2(tx, writeSet, "value")

	_, err := executor.levm.CallContract(*tx.Tx.EthTx.From(), *tx.Tx.EthTx.To(), tx.Tx.EthTx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("Transaction Execute", err)

	//executor.Execute(tx)
	//tx.Execute()
	tx.Committed.Store(true)
	//// atomic.AddUint64(&executor.counter, 1)
	//fmt.Printf("tx %d committed", tx.ID)
}

// / @brief clean up the lock table
func (executor *HarmonyExecutor) CleanLockTable(tx *HarmonyTransaction) {
	for key := range tx.LocalPut {

		executor.lockTable.table.Put(key, func(entry *HarmonyLockEntry) {
			(*entry).DepsPut = nil
			// 或者使用空切片：entry.deps_put = make([]*T, 0)
		})
	}
	for key := range tx.LocalGet {
		executor.lockTable.table.Put(key, func(entry *HarmonyLockEntry) {
			(*entry).DepsGet = nil
			// 或者使用空切片：entry.deps_get = make([]*T, 0)
		})
	}
}

// handleGetStorage 处理读存储操作
func (he *HarmonyExecutor) ExecutorGetStorage2(tx *HarmonyTransaction, readSet map[string]bool) {
	var keys string
	for key := range readSet {
		keys += key + " "
		var value string
		he.table.table.Get(key, func(entry HarmonyEntry) {
			value = entry.Value
		})
		tx.LocalGet[key] = value
	}
	//fmt.Printf("tx %s fallbacking, read: %s", tx.ID, keys)
}

func (he *HarmonyExecutor) ExecutorSetStorage2(tx *HarmonyTransaction, writeSet map[string]bool, value string) {
	var keys string
	for key := range writeSet {
		keys += key + " "
		he.table.table.Put(key, func(entry *HarmonyEntry) {
			(*entry).Value = value
		})
	}
	//fmt.Printf("tx %s fallbacking, write: %s", tx.ID, keys)
}

// handleGetStorage 处理读存储操作
func (he *HarmonyExecutor) ExecutorGetStorage(tx *HarmonyTransaction, readSet map[string]bool) {
	var keys []string
	for key := range readSet {
		keys = append(keys, key)
		value := ""

		he.table.table.Put(key, func(entry *HarmonyEntry) {
			value = (*entry).Value
			// update put_txs' max_in and tx's min_out
			for _, _tx := range (*entry).ReservedPutTxs {
				if _tx == tx {
					continue
				}
				he.table.OnSeeingRWDependency(_tx, tx)
			}
			(*entry).ReservedGetTxs = append((*entry).ReservedGetTxs, tx)
		})
		tx.LocalGet[key] = value
	}
	//fmt.Printf("tx %s read: %s", tx.ID, strings.Join(keys, " "))
}

// handleSetStorage 处理写存储操作
func (he *HarmonyExecutor) ExecutorSetStorage(tx *HarmonyTransaction, writeSet map[string]bool, value string) {
	var keys []string
	for key := range writeSet {
		keys = append(keys, key)
		tx.LocalPut[key] = value
		he.table.table.Put(key, func(entry *HarmonyEntry) {
			// update get_txs' min_out and tx's max_in
			for _, _tx := range (*entry).ReservedGetTxs {
				if _tx == tx {
					continue
				}
				he.table.OnSeeingRWDependency(tx, _tx)
			}
			(*entry).ReservedPutTxs = append((*entry).ReservedPutTxs, tx)
		})
	}
	//log.Printf("tx %s write: %s", tx.ID, strings.Join(keys, " "))
}
