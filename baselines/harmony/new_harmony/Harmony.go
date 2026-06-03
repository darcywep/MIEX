package newHarmony

import (
	"Janus/baselines/common"
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
	barrier          *HarmonyBarrier
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
	onCompletion func()
	parties      int
	mu           sync.Mutex
	current      int
	generation   int // ← 新增: 代次计数器
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

	gen := b.generation // 记住当前代次
	b.current++

	if b.current == b.parties {
		// 最后一个到达: 重置并推进代次
		b.current = 0
		b.generation++ // 推进代次
		if b.onCompletion != nil {
			b.onCompletion()
		}
		b.cond.Broadcast()
	} else {
		// 在循环中等待，直到代次变化
		// 这同时解决了虚假唤醒和代次混淆两个问题
		for gen == b.generation {
			b.cond.Wait()
		}
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

	batches := make([][][]*HarmonyTransaction, 0, len(h.blocks))

	for i, block := range h.blocks {
		txs := block.GetTxs()
		txPerThread := 1
		index := 0

		batch := make([][]*HarmonyTransaction, h.numThreads)
		batchID := i + 1

		for j := 0; j < len(txs); j += txPerThread {
			batchIdx := index % h.numThreads

			for k := 0; k < txPerThread && j+k < len(txs); k++ {
				tx := txs[j+k]
				txID := tx.Txid
				txInner := tx
				batch[batchIdx] = append(batch[batchIdx], NewHarmonyTransaction(txInner, txID, uint32(batchID), i, tx.OriginalTxID))
			}
			index++
		}

		batches = append(batches, batch)
		h.Statistics.JournalBlock()
	}

	var wg sync.WaitGroup

	h.levm = levm.Copy()

	for i := 0; i < h.numThreads; i++ {
		threadBatches := make([][]*HarmonyTransaction, 0, len(h.blocks))
		for j := 0; j < len(h.blocks); j++ {
			threadBatches = append(threadBatches, batches[j][i])
		}

		wg.Add(1)
		worker := NewHarmonyExecutor(h, uint32(i), threadBatches, levm.Copy())
		h.workers[i] = worker

		go func(workerID int, executor *HarmonyExecutor) {
			runtime.LockOSThread()
			defer wg.Done()
			executor.Run()
		}(i, worker)
	}
	wg.Wait()

	for _, worker := range h.workers {
		worker.levm.AllDB().StateDB.FlushDirtyToNewStateDB(levm.AllDB().StateDB)
	}
}

// Run 执行交易
func (e *HarmonyExecutor) Run() {

	if e.enableInterBlock {
		fmt.Printf("worker %d size of batchTxs %d", e.workerID, len(e.batchTxs))
		e.barrier.ArriveAndWait()
		e.InterBlockExecute(e.NextBatch())
	} else {
		for _, batch := range e.batchTxs {
			// stage 1: execute (simulation step)
			_stop := e.confirmExit.Load() == int32(e.numThreads)
			e.barrier.ArriveAndWait()
			if _stop {
				return
			}
			if e.stopFlag.Load() {
				addr := e.confirmExit.Load()
				atomic.CompareAndSwapInt32(&addr, int32(e.workerID), int32(e.workerID+1))
			}
			//fmt.Printf("worker %d executing\n", e.workerID)

			for i := range batch {
				tx := &batch[i]
				//fmt.Printf("worker %d executing, execut txid %d, batchID %d, %d\n", e.workerID, (*tx).originalTxID, (*tx).BatchID, len(batch))
				(*tx).StartTime = time.Now()
				e.Execute(*tx)
				e.statistics.JournalExecute()
			}

			// stage 2: verify + commit
			e.barrier.ArriveAndWait()

			var beginTime time.Time
			if e.workerID == 0 {
				beginTime = time.Now()
			}

			// 阶段 2a: 所有 worker 并行 Verify
			for i := range batch {
				tx := &batch[i]
				e.Verify(*tx)
				if (*tx).FlagConflict {
					//fmt.Printf("worker %d executing, abort txid %d, batchID %d\n", e.workerID, (*tx).originalTxID, (*tx).BatchID)
					e.PrepareLockTable(*tx)
					if tools.TraceAbort {
						tools.TraceAbortMutex.Lock()
						ariaAbortTxs[(*tx).BlockID][(*tx).originalTxID] = *tx
						tools.TraceAbortMutex.Unlock()
					}
				}
			}

			// 确保所有 worker 的 Verify 都完成，
			// 这样 ApplyWriteSets 中 filter FlagConflict 时看到的是最终状态
			e.barrier.ArriveAndWait()
			//fmt.Println()

			// 阶段 2b: 所有 worker 并行 Commit
			for i := range batch {
				tx := &batch[i]
				if !(*tx).FlagConflict {
					e.CommitWithReordering(*tx)
					(*tx).Committed.Store(true)
					latency := time.Since((*tx).StartTime).Microseconds()
					e.statistics.JournalCommit(uint32(latency))
				}
			}

			//for i := range batch {
			//	tx := &batch[i]
			//	e.Verify(*tx)
			//	if (*tx).FlagConflict {
			//		e.PrepareLockTable(*tx)
			//		if tools.TraceAbort {
			//			tools.TraceAbortMutex.Lock()
			//			ariaAbortTxs[(*tx).BlockID][(*tx).originalTxID] = *tx
			//			tools.TraceAbortMutex.Unlock()
			//		}
			//	} else {
			//		// ========== 修改点 9: 用 CommitWithReordering 替代 Commit ==========
			//		// 原代码: e.Commit(*tx)
			//		// 原因: 原 Commit 直接将 LocalPut 写入 table，
			//		// 写入顺序取决于 worker 执行顺序 → 不确定。
			//		//
			//		// 论文 Algorithm 1 Line 15: Apply_write_sets(Tj)
			//		// → 调用 Algorithm 2 的 Apply_write_sets 函数
			//		// → 按 min_out 排序 → coalescence → 写入
			//		//
			//		// 论文 Theorem 1 + Rule 2 证明:
			//		// 只有 reorder 后，完整依赖图 (含 ww/wr) 才保证无环。
			//		// 不做 reorder = 可序列化性不保证 = 程序正确性不保证。
			//		e.CommitWithReordering(*tx)
			//
			//		// ========== 修改点 10: Commit 后设置 Committed 标志 ==========
			//		// 原因: Fallback 阶段中:
			//		//   for !should_wait.Committed.Load() {
			//		//       runtime.Gosched()
			//		//   }
			//		// 这个 busy-wait 依赖 should_wait.Committed 被设为 true。
			//		//
			//		// 原代码: Commit 路径没有设置 Committed=true
			//		// → 如果 Fallback 中等待的事务通过正常 Commit 路径提交,
			//		//   Committed 永远为 false → Fallback 死循环。
			//		//
			//		// Fallback 路径已有: tx.Committed.Store(true)
			//		// 但 Commit 路径缺失，两条路径不一致。
			//		(*tx).Committed.Store(true)
			//
			//		latency := time.Since((*tx).StartTime).Microseconds()
			//		e.statistics.JournalCommit(uint32(latency))
			//	}
			//}

			// stage 3: fallback
			e.barrier.ArriveAndWait()

			//fmt.Printf("worker %d fallbacking \n", e.workerID)

			for i := range batch {
				tx := &batch[i]
				if (*tx).FlagConflict {
					e.Fallback(*tx)
					e.statistics.JournalExecute()
					latency := time.Since((*tx).StartTime).Microseconds()
					e.statistics.JournalCommit(uint32(latency))
				}
			}

			// stage 4: clean up
			e.barrier.ArriveAndWait()
			if e.workerID == 0 {
			}
			//fmt.Printf("worker %d cleaning up \n", e.workerID)

			for i := range batch {
				tx := &batch[i]
				e.CleanLockTable(*tx)
				// ========== 修改点 11: 清理 HarmonyEntry 残留状态 ==========
				// 原因: HarmonyEntry 中的 ReservedGetTxs, ReservedPutTxs,
				// UpdateCmds, Handled 都是 block 级别的生命周期。
				//
				// 不清理的后果:
				// 下一个 block 的事务在 ExecutorGetStorage 中遍历 ReservedPutTxs,
				// 会看到上一个 block 的写事务 → 建立错误的 rw-dependency
				// → MinOut/MaxIn 被错误更新 → 错误的 abort 判定
				//
				// 原代码只清理了 LockTable，没有清理 HarmonyTable。
				for key := range (*tx).LocalGet {
					e.table.ResetEntry(key)
				}
				for key := range (*tx).LocalPut {
					e.table.ResetEntry(key)
				}
			}

			//elapsed := time.Since(beginTime)
			_ = time.Since(beginTime)

			//fmt.Printf("CommitCount= %d \n", e.statistics.CommitCount.Load())
			//fmt.Printf("交易实际被执行总次数 %d \n", e.statistics.ExecCount.Load())
			//fmt.Printf("交易处理吞吐(TPS)= %f \n", float64(e.statistics.CommitCount.Load())/(elapsed.Seconds()))
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
func (e *HarmonyExecutor) InterBlockExecute(batch []*HarmonyTransaction) {
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

	for i := range batch {
		tx := &batch[i]
		(*tx).StartTime = time.Now()
		e.Execute(*tx)
		e.statistics.JournalExecute()
	}

	// stage 2: verify + commit
	e.barrier.ArriveAndWait()

	conflictNum := 0

	// 阶段 2a: Verify (修改点 15 同上)
	for i := range batch {
		tx := &batch[i]
		e.Verify(*tx)
		if (*tx).FlagConflict {
			conflictNum++
			e.PrepareLockTable(*tx)
		}
	}

	// 等待所有 worker 完成 Verify
	e.barrier.ArriveAndWait()

	// 阶段 2b: Commit
	for i := range batch {
		tx := &batch[i]
		if !(*tx).FlagConflict {
			e.CommitWithReordering(*tx)
			(*tx).Committed.Store(true)
			latency := time.Since((*tx).StartTime).Microseconds()
			e.statistics.JournalCommit(uint32(latency))
		}
	}

	//conflictNum := 0
	//
	//for i := range batch {
	//	tx := &batch[i]
	//	e.Verify(*tx)
	//
	//	if (*tx).FlagConflict {
	//		conflictNum++
	//	}
	//
	//	if (*tx).FlagConflict {
	//		e.PrepareLockTable(*tx)
	//	} else {
	//		// 修改点 9 (同上)
	//		e.CommitWithReordering(*tx)
	//		// 修改点 10 (同上)
	//		(*tx).Committed.Store(true)
	//		latency := time.Since((*tx).StartTime).Microseconds()
	//		e.statistics.JournalCommit(uint32(latency))
	//	}
	//}

	// stage 3: fallback
	e.barrier.ArriveAndWait()

	for i := range batch {
		tx := &batch[i]
		if (*tx).FlagConflict {
			e.Fallback(*tx)
			e.statistics.JournalExecute()
			latency := time.Since((*tx).StartTime).Microseconds()
			e.statistics.JournalCommit(uint32(latency))
		}
	}

	// stage 4
	//if e.batchIdx < uint32(len(e.batchTxs)) {
	//	e.counter.Add(1)
	//
	//	if e.counter.Load() == int32(e.numThreads) {
	//		e.counter.Store(0)
	//	}
	//	e.InterBlockExecute(e.NextBatch())
	//} else {
	//	e.barrier.ArriveAndWait()
	//	if e.workerID == 0 {
	//	}
	//	fmt.Printf("worker %d cleaning up batch %d \n", e.workerID, e.batchIdx)
	//	for i := range batch {
	//		tx := &batch[i]
	//		e.CleanLockTable(*tx)
	//		// 修改点 11 (同上)
	//		for key := range (*tx).LocalGet {
	//			e.table.ResetEntry(key)
	//		}
	//		for key := range (*tx).LocalPut {
	//			e.table.ResetEntry(key)
	//		}
	//	}
	//}
	if e.batchIdx < uint32(len(e.batchTxs)) {
		// 清理当前 batch（在递归到下一个 batch 之前）
		e.counter.Add(1)
		if e.counter.Load() == int32(e.numThreads) {
			e.counter.Store(0)
		}

		// ← 在这里加 barrier 和清理
		e.barrier.ArriveAndWait()
		for i := range batch {
			tx := &batch[i]
			e.CleanLockTable(*tx)
			for key := range (*tx).LocalGet {
				e.table.ResetEntry(key)
			}
			for key := range (*tx).LocalPut {
				e.table.ResetEntry(key)
			}
		}

		e.InterBlockExecute(e.NextBatch())
	} else {
		e.barrier.ArriveAndWait()
		for i := range batch {
			tx := &batch[i]
			e.CleanLockTable(*tx)
			// 修改点 11 (同上)
			for key := range (*tx).LocalGet {
				e.table.ResetEntry(key)
			}
			for key := range (*tx).LocalPut {
				e.table.ResetEntry(key)
			}
		}
	}
}

// Execute 模拟阶段: 执行交易并建立 rw-dependency
// 对应论文 Section 3.1: simulation step
func (executor *HarmonyExecutor) Execute(tx *HarmonyTransaction) {

	if !tools.ExecuteSimulatedTransaction(tx.Tx.EthTx) {
		_, err := executor.levm.CallContract(*tx.Tx.EthTx.From(), *tx.Tx.EthTx.To(), tx.Tx.EthTx.Data(), new(uint256.Int).SetUint64(0))
		tools.PanicError("Transaction Execute", err)
	}

	// 真实负载直接使用 LatencyDB 读写集；合成负载仍回退到 SmallBank 规则。
	tools.FillStringReadWriteSet(tx.Tx.EthTx, tx.Tx.Vertex.ReadKeys, tx.Tx.Vertex.WriteKeys)

	readSet := make(map[string]bool)
	writeSet := make(map[string]bool)

	for key := range tx.Tx.Vertex.ReadKeys {
		readSet[key] = true
	}
	executor.ExecutorGetStorage(tx, readSet)

	for key := range tx.Tx.Vertex.WriteKeys {
		writeSet[key] = true
	}
	executor.ExecutorSetStorage(tx, writeSet, "value")
}

// ========== 修改点 12: Verify 函数 — 修复 inter-block Rule 3 ==========
//
// 原代码:
//
//	if executor.enableInterBlock {
//	    if tx.MinOut < tx.ID && tx.MinOut <= tx.MaxIn && tx.BatchID == tx.InBatchID {
//	        tx.FlagConflict = true
//	    } else if tx.MinOut < tx.ID && tx.OutBatchID < tx.BatchID {
//	        tx.FlagConflict = true
//	    }
//	}
//
// 问题 A: 条件2 abort 了 Tj (当前事务), 但论文 Rule 3(ii) 要求 abort Tk。
//
//	论文 Rule 3:
//	  (i)  if Tj and Tk are in the same block, abort Tj
//	  (ii) otherwise, abort Tk
//
//	在"每个事务自检"的模型中，Rule 3(ii) 需要在 Tk 的视角检测:
//	  Tk 发现自己有出边 (MinOut < ID)，且出边目标在不同 block (OutBatchID != BatchID)，
//	  且自己有入边形成 backward dangerous structure (MinOut <= MaxIn)
//	  → Tk abort 自己 (对应 Rule 3(ii) 的 "abort Tk")
//
//	原代码条件2: tx.MinOut < tx.ID && tx.OutBatchID < tx.BatchID
//	这里 tx 是 Tj 的视角:
//	  "我有 backward 出边，且出边跨 block" → abort 自己 (Tj)
//	但论文说跨 block 时应该 abort Tk (入边来源)，不是 abort Tj。
//
// 问题 B: 条件2 缺少 MinOut <= MaxIn 的检查。
//
//	没有入边 (MaxIn == MinInt64) 时也会 abort → 过度丢弃。
//
// 修复: 重写 inter-block 验证逻辑。
func (executor *HarmonyExecutor) Verify(tx *HarmonyTransaction) {

	if executor.enableInterBlock {
		// ---- Rule 3(i): Tj 和 Tk 在同一 block ----
		// backward dangerous structure: T_min_out ←rw— Tj ←rw— T_max_in
		// min_out < j: 有 backward 出边
		// min_out <= max_in: 有入边且形成 dangerous structure
		// BatchID == InBatchID: Tk (入边来源) 和 Tj 在同一 block
		// → abort Tj (即当前 tx)
		if tx.MinOut < int64(tx.ID) && tx.MinOut <= tx.MaxIn && tx.BatchID == tx.InBatchID {
			tx.FlagConflict = true
		}

		// ---- Rule 3(ii): Tj 和 Tk 不在同一 block ----
		// 在 Tk 的视角做检测 (当前 tx 扮演 Tk 的角色):
		// 当前 tx 有 backward 出边 (MinOut < ID)
		// 且出边目标在更早的 block (OutBatchID < BatchID) — 跨 block
		// 且自己��入边形成 dangerous structure (MinOut <= MaxIn)
		// 且入边来源 (Tj) 与自己在不同 block (InBatchID != BatchID)
		// → abort Tk (即当前 tx 自己)
		if !tx.FlagConflict {
			if tx.MinOut < int64(tx.ID) && tx.MinOut <= tx.MaxIn && tx.BatchID != tx.InBatchID {
				tx.FlagConflict = true
			}
		}

		// 额外保守检查: 出边跨 block 且有入边存在
		// 这对应论文中 generalized backward dangerous structure
		// 跨越多个 block 的情况
		if !tx.FlagConflict {
			if tx.MinOut < int64(tx.ID) && tx.OutBatchID < tx.BatchID && tx.MaxIn > MaxInInitValue {
				tx.FlagConflict = true
			}
		}

	} else {
		// ---- 无 inter-block: 标准 Rule 1 ----
		// 论文 Algorithm 1 Line 12:
		// if Tj.min_out < j and Tj.min_out ≤ Tj.max_in then Abort(Tj)
		//
		// 原代码: if tx.MinOut < tx.ID && tx.MinOut <= tx.MaxIn
		// 类型已从 uint32 改为 int64, 逻辑不变, 但 MaxIn 初始值
		// 从 0 改为 MinInt64, 修复了无入边时的误判。
		if tx.MinOut < int64(tx.ID) && tx.MinOut <= tx.MaxIn {
			tx.FlagConflict = true
		}
	}

	if tx.FlagConflict {
		if tools.TraceAbort {
			tools.TraceAbortMutex.Lock()
			ariaAbortTxs[tx.BlockID][tx.originalTxID] = tx
			tools.TraceAbortMutex.Unlock()
		}
	}
}

// ========== 修改点 9: 新增 CommitWithReordering ==========
// 原因: 替代原始 Commit, 走论文 Algorithm 2 的 Apply_write_sets 路径。
// 详细原因见 Utils.go 中 ApplyWriteSets 的注释。
func (executor *HarmonyExecutor) CommitWithReordering(tx *HarmonyTransaction) {
	executor.table.ApplyWriteSets(tx)
}

// Commit 保留原始简单提交, 仅供 Fallback 使用。
// Fallback 阶段已按依赖顺序串行执行, 不需要 reordering。
func (executor *HarmonyExecutor) Commit(tx *HarmonyTransaction) {
	for key, value := range tx.LocalPut {
		executor.table.table.Put(key, func(entry *HarmonyEntry) {
			(*entry).Value = value
		})
	}
}

// PrepareLockTable 将冲突事务注册到锁表供 Fallback 使用
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

// ========== 修改点 13: Fallback 中修正 读→执行→写 顺序 ==========
//
// 原代码顺序:
//
//	executor.ExecutorGetStorage2(tx, readSet)     // 读
//	executor.ExecutorSetStorage2(tx, writeSet, "value") // 写
//	executor.levm.CallContract(...)               // 执行
//
// 问题: 合约执行需要基于最新读取的状态值来计算写入结果。
// 原代码在执行合约之前就已经将固定值 "value" 写入了 table，
// 导致:
//
//	(a) 合约执行无法读到正确的状态
//	(b) 合约计算出的实际结果没有被写入 table
//	(c) 写入的值始终是硬编码的 "value", 与合约语义无关
//
// 正确顺序: 读 → 执行合约 → 写
func (executor *HarmonyExecutor) Fallback(tx *HarmonyTransaction) {

	// 找到应等待的最近依赖事务
	var should_wait *HarmonyTransaction = nil
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
			runtime.Gosched()
		}
		//if tools.TraceAbort {
		//	tools.TraceAbortMutex.Lock()
		//	delete(ariaAbortTxs[should_wait.BlockID], should_wait.originalTxID)
		//	tools.TraceAbortMutex.Unlock()
		//}
	}

	readSet := make(map[string]bool)
	for key := range tx.LocalGet {
		readSet[key] = true
	}

	writeSet := make(map[string]bool)
	for key := range tx.LocalPut {
		writeSet[key] = true
	}

	// ===== 正确顺序: 读 → 执行 → 写 =====

	// Step 1: 从 table 读取最新已提交的值
	executor.ExecutorGetStorage2(tx, readSet)

	// Step 2: 基于最新值重新执行合约逻辑
	if !tools.ExecuteSimulatedTransaction(tx.Tx.EthTx) {
		_, err := executor.levm.CallContract(*tx.Tx.EthTx.From(), *tx.Tx.EthTx.To(), tx.Tx.EthTx.Data(), new(uint256.Int).SetUint64(0))
		tools.PanicError("Transaction Fallback Execute", err)
	}

	// Step 3: 将执行结果写入 table
	executor.ExecutorSetStorage2(tx, writeSet, "value")

	tx.Committed.Store(true)
}

// CleanLockTable 清理锁表
func (executor *HarmonyExecutor) CleanLockTable(tx *HarmonyTransaction) {
	for key := range tx.LocalPut {
		executor.lockTable.table.Put(key, func(entry *HarmonyLockEntry) {
			(*entry).DepsPut = nil
		})
	}
	for key := range tx.LocalGet {
		executor.lockTable.table.Put(key, func(entry *HarmonyLockEntry) {
			(*entry).DepsGet = nil
		})
	}
}

// ExecutorGetStorage2 Fallback 阶段的读操作 (读已提交值)
func (he *HarmonyExecutor) ExecutorGetStorage2(tx *HarmonyTransaction, readSet map[string]bool) {
	for key := range readSet {
		var value string
		he.table.table.Get(key, func(entry HarmonyEntry) {
			value = entry.Value
		})
		tx.LocalGet[key] = value
	}
}

// ExecutorSetStorage2 Fallback 阶段的写操作 (直接写入)
func (he *HarmonyExecutor) ExecutorSetStorage2(tx *HarmonyTransaction, writeSet map[string]bool, value string) {
	for key := range writeSet {
		he.table.table.Put(key, func(entry *HarmonyEntry) {
			(*entry).Value = value
		})
	}
}

// ExecutorGetStorage 模拟阶段的读操作 — 建立 rw-dependency
// 论文 Algorithm 1 Line 6: on_seeing_rw_dependency
//
// 当前 tx 读了 key, entry.ReservedPutTxs 中是之前"reserved"要写该 key 的事务。
// 在模拟阶段所有事务读 block snapshot, 所以 tx 读到的是旧值 (before-image),
// 而 _tx 要写新值。
//
// 依赖关系: tx 读了 _tx 写的 before-image
//
//	→ tx rw-depends on _tx
//	→ _tx ←rw— tx (论文箭头方向)
//	→ OnSeeingRWDependency(Ti=_tx, Tj=tx)
//	→ tx.MinOut = min(_tx.ID, tx.MinOut)  // tx 的出边
//	→ _tx.MaxIn = max(tx.ID, _tx.MaxIn)  // _tx 的入边
func (he *HarmonyExecutor) ExecutorGetStorage(tx *HarmonyTransaction, readSet map[string]bool) {
	for key := range readSet {
		value := ""

		he.table.table.Put(key, func(entry *HarmonyEntry) {
			value = (*entry).Value

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
}

// ExecutorSetStorage 模拟阶段的写操作 — 建立 rw-dependency + 注册 update command
// 论文 Algorithm 1 Line 6 + Algorithm 2 Line 2-5
//
// 当前 tx 写了 key, entry.ReservedGetTxs 中是之前读过该 key 的事务。
// _tx 之前读了 key 的旧值, tx 现在要写 key。
//
// 依赖关系: _tx 读了 tx 写的 before-image
//
//	→ _tx rw-depends on tx
//	→ tx ←rw— _tx (论文箭头方向)
//	→ OnSeeingRWDependency(Ti=tx, Tj=_tx)
//	→ _tx.MinOut = min(tx.ID, _tx.MinOut)  // _tx 的出边
//	→ tx.MaxIn = max(_tx.ID, tx.MaxIn)     // tx 的入边
func (he *HarmonyExecutor) ExecutorSetStorage(tx *HarmonyTransaction, writeSet map[string]bool, value string) {
	for key := range writeSet {
		tx.LocalPut[key] = value
		he.table.table.Put(key, func(entry *HarmonyEntry) {
			for _, _tx := range (*entry).ReservedGetTxs {
				if _tx == tx {
					continue
				}
				he.table.OnSeeingRWDependency(tx, _tx)
			}
			(*entry).ReservedPutTxs = append((*entry).ReservedPutTxs, tx)

			// ========== 修改点 14: 注册 UpdateCommand ==========
			// 原因: 论文 Algorithm 2 Line 2-5:
			//   Event on_update(key, update_command):
			//     update_cmds ← update_reservation.search(key)
			//     update_cmds.append(update_command)
			//     T_current.updated_keys.append(key)
			//
			// 原代码完全缺少此步骤。没有注册 update command,
			// ApplyWriteSets 就无法获取到该 key 上的所有更新,
			// 无法进行 reordering 和 coalescence。
			entry.UpdateCmds = append(entry.UpdateCmds, &UpdateCommand{
				Tx:    tx,
				Key:   key,
				Value: value,
			})
		})
		// 记录 updated_keys (论文 Algorithm 2 Line 5)
		tx.UpdatedKeys = append(tx.UpdatedKeys, key)
	}
}
