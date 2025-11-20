package aria

import (
	optmeCommon "Janus/baselines/common"
	"Janus/config"
	lvm "Janus/core/evm"
	"sync/atomic"
	"time"
)

// AriaExecutor 负责在不同阶段执行事务批次
type AriaExecutor struct {
	levm          *lvm.LEVM
	statistics    *optmeCommon.Statistics
	batchTxs      [][]*AriaTransaction
	table         *AriaTable
	lockTable     *AriaLockTable
	enableReorder bool
	numThreads    int
	confirmExit   *atomic.Int64
	stopFlag      *atomic.Bool
	barrier       *Barrier
	counter       *atomic.Int64
	hasConflict   *atomic.Bool
	workerID      int
}

// NewAriaExecutor 创建执行器
func NewAriaExecutor(aria *Aria, levm *lvm.LEVM, workerID int, batchTxs [][]*AriaTransaction) *AriaExecutor {
	return &AriaExecutor{
		levm:          levm,
		statistics:    aria.statistics,
		batchTxs:      batchTxs,
		table:         aria.table,
		lockTable:     aria.lockTable,
		enableReorder: aria.enableReorder,
		numThreads:    aria.numThreads,
		confirmExit:   &aria.confirmExit,
		stopFlag:      &aria.stopFlag,
		barrier:       aria.barrier,
		counter:       &aria.counter,
		hasConflict:   &aria.hasConflict,
		workerID:      workerID,
	}
}

// Run 执行完整的 Aria 协议阶段
func (e *AriaExecutor) Run() {
	for _, batch := range e.batchTxs {

		e.barrier.Wait()
		if e.confirmExit.Load() == int64(e.numThreads) {
			return
		}
		if e.stopFlag.Load() {
			e.confirmExit.Add(1)
			return
		}
		e.hasConflict.Store(false)

		//fmt.Println("worker", e.workerID, "start batch with", len(batch), "txs")
		// -------- Stage 1: Execute + Reserve --------
		for _, tx := range batch {
			tx.StartTime = time.Now()
			e.Execute(tx)
			e.Reserve(tx)
			e.statistics.JournalExecute()
			//e.statistics.JournalOverheads(tx.CountOverheads())
		}

		// -------- Stage 2: Verify + Commit/Fallback prepare --------
		e.barrier.Wait()
		//fmt.Println("worker", e.workerID, "verifying")
		beginTime := time.Time{}
		if e.workerID == 0 {
			beginTime = time.Now()
		}
		for _, tx := range batch {
			e.Verify(tx)
			if tx.flagConflict.Load() {
				e.hasConflict.Store(true)
				e.PrepareLockTable(tx)
			} else {
				e.Commit(tx)
				e.statistics.JournalCommit(uint32(time.Since(tx.StartTime).Microseconds()))
			}
		}

		// -------- Stage 3: Fallback --------
		e.barrier.Wait()
		if e.workerID == 0 {
			e.statistics.JournalRollbackExecution(uint32(time.Since(beginTime).Microseconds()))
			beginTime = time.Now()
		}
		if !e.hasConflict.Load() {
			continue
		}
		//fmt.Println("before fallback")
		for _, tx := range batch {
			if tx.flagConflict.Load() {
				e.Fallback(tx)
				e.statistics.JournalExecute()
				//e.statistics.JournalCommit(uint32(time.Since(tx.StartTime).Microseconds()))
				e.statistics.AddRollbackCount()
			}
		}

		// -------- Stage 4: Cleanup --------
		e.barrier.Wait()
		if e.workerID == 0 {
			e.statistics.JournalRollbackExecution(uint32(time.Since(beginTime).Microseconds()))
		}
		for _, tx := range batch {
			e.CleanLockTable(tx)
		}
	}
}

// Execute 执行事务逻辑并登记读写集
func (e *AriaExecutor) Execute(tx *AriaTransaction) {
	// 模拟 snapshot read handler

	// 需要记录读写
	if tx.Inner.EthTx.TxType == config.IOTx {
		key1 := tx.Inner.EthTx.From().String()
		key2 := tx.Inner.EthTx.SmallBankTo.String()
		tx.Inner.Vertex.WriteKeys[key1] = "value"
		tx.Inner.Vertex.WriteKeys[key2] = "value"
		tx.Inner.Vertex.ReadKeys[key2] = "value"
		tx.Inner.Vertex.ReadKeys[key2] = "value"

		tx.LocalPut[key1] = "value"
		tx.LocalPut[key2] = "value"
		tx.LocalGet[key1] = "value"
		tx.LocalGet[key2] = "value"

	} else {
		key1 := tx.Inner.EthTx.SmallBankTo.String()
		tx.Inner.Vertex.WriteKeys[key1] = "value"
		tx.Inner.Vertex.ReadKeys[key1] = "value"

		tx.LocalPut[key1] = "value"
		tx.LocalGet[key1] = "value"
	}
	////// 模拟 snapshot read handler
	//for key, _ := range tx.LocalGet {
	//	e.table.Table.Get(key, func(entry AriaEntry) {
	//		_ = entry.Value
	//	})
	//}
	//for key, value := range tx.LocalPut {
	//	tx.LocalPut[key] = value
	//}

	tx.Execute(e.levm) // 执行用户逻辑
}

// Reserve 把事务的读写集登记进表
func (e *AriaExecutor) Reserve(tx *AriaTransaction) {
	for key := range tx.LocalGet {
		e.table.ReserveGet(tx, key)
	}
	for key := range tx.LocalPut {
		e.table.ReservePut(tx, key)
	}
}

// Verify 检查事务依赖冲突
func (e *AriaExecutor) Verify(tx *AriaTransaction) {
	var war, waw, raw bool

	for key := range tx.LocalGet {
		raw = raw || !e.table.CompareReservedPut(tx, key)
	}
	for key := range tx.LocalPut {
		war = war || !e.table.CompareReservedGet(tx, key)
		waw = waw || !e.table.CompareReservedPut(tx, key)
	}

	if e.enableReorder {
		tx.flagConflict.Store(waw || (raw && war))
	} else {
		tx.flagConflict.Store(waw || raw)
	}

	if tx.flagConflict.Load() {
		//log.Printf("Abort tx %d raw:%v war:%v waw:%v", tx.ID, raw, war, waw)
	}
}

// Commit 提交事务结果到表
func (e *AriaExecutor) Commit(tx *AriaTransaction) {
	for key, value := range tx.LocalPut {
		e.table.Table.Put(key, func(entry *AriaEntry) {
			(*entry).Value = value
		})
	}
	tx.committed.Store(1)
}

// PrepareLockTable 填充悲观锁依赖表
func (e *AriaExecutor) PrepareLockTable(tx *AriaTransaction) {
	for key := range tx.LocalGet {
		e.lockTable.AddGetDep(key, tx)
	}
	for key := range tx.LocalPut {
		e.lockTable.AddPutDep(key, tx)
	}
}

// Fallback 悲观回退执行（顺序保证）
func (e *AriaExecutor) Fallback(tx *AriaTransaction) {
	for key, _ := range tx.LocalGet {
		e.table.Table.Get(key, func(entry AriaEntry) {
			_ = entry.Value
		})
	}

	for key, value := range tx.LocalPut {
		e.table.Table.Put(key, func(entry *AriaEntry) {
			(*entry).Value = value
		})
	}

	var shouldWait *AriaTransaction
	for key := range tx.LocalPut {
		entry := e.lockTable.GetEntry(key)
		for _, dep := range entry.DepsGet {
			if dep.ID < tx.ID && (shouldWait == nil || dep.ID > shouldWait.ID) {
				shouldWait = dep
			}
		}
		for _, dep := range entry.DepsPut {
			if dep.ID < tx.ID && (shouldWait == nil || dep.ID > shouldWait.ID) {
				shouldWait = dep
			}
		}
	}
	for key := range tx.LocalGet {
		entry := e.lockTable.GetEntry(key)
		for _, dep := range entry.DepsPut {
			if dep.ID < tx.ID && (shouldWait == nil || dep.ID > shouldWait.ID) {
				shouldWait = dep
			}
		}
	}

	for shouldWait != nil && !shouldWait.IsCommitted() {
		time.Sleep(50 * time.Microsecond)
	}

	tx.Execute(e.levm)
	tx.committed.Store(1)
}

// CleanLockTable 清除事务的依赖
func (e *AriaExecutor) CleanLockTable(tx *AriaTransaction) {
	for key := range tx.LocalPut {
		e.lockTable.ClearDeps(key)
	}
	for key := range tx.LocalGet {
		e.lockTable.ClearDeps(key)
	}
}
