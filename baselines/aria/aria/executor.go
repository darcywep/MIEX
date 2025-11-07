package aria

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic
	"time"
)

// -----------------------------
// AriaExecutor: 执行器，用于并行处理事务
// -----------------------------
type AriaExecutor struct {
	stats         *Statistics
	batchTxs      [][]*AriaTransaction
	table         *AriaTable
	lockTable     *AriaLockTable
	enableReorder bool
	numThreads    int
	confirmExit   *int32
	stopFlag      *atomic.Bool
	barrier       *CyclicBarrier
	counter       *int32
	hasConflict   *atomic.Bool
	workerID      int
	aria          *Aria
}

func NewAriaExecutor(a *Aria, workerID int, batchTxs [][]*AriaTransaction) *AriaExecutor {
	return &AriaExecutor{
		stats:         a.statistics,
		batchTxs:      batchTxs,
		table:         a.table,
		lockTable:     a.lockTable,
		enableReorder: a.enableReorder,
		numThreads:    a.numThreads,
		confirmExit:   &a.confirmExit,
		stopFlag:      &a.stopFlag,
		barrier:       a.barrier,
		counter:       &a.counter,
		hasConflict:   &a.hasConflict,
		workerID:      workerID,
		aria:          a,
	}
}

func (ex *AriaExecutor) Run() {
	for _, batch := range ex.batchTxs {
		_stop := atomic.LoadInt32(ex.confirmExit) == int32(ex.numThreads)
		ex.barrier.ArriveAndWait()
		if _stop {
			return
		}
		if ex.stopFlag.Load() {
			atomic.AddInt32(ex.confirmExit, 1)
		}
		ex.hasConflict.Store(false)
		for _, tx := range batch {
			tx.startTime = time.Now()
			ex.Execute(tx)
			ex.Reserve(tx)
			ex.stats.JournalExecute()
			ex.stats.JournalOverheads(tx.CountOverheads())
		}

		ex.barrier.ArriveAndWait()

		for _, tx := range batch {
			ex.Verify(tx)
			if tx.flagConflict {
				ex.hasConflict.Store(true)
				ex.PrepareLockTable(tx)
			} else {
				ex.Commit(tx)
				latency := int64(time.Since(tx.startTime) / time.Microsecond)
				ex.stats.JournalCommit(latency)
			}
		}

		ex.barrier.ArriveAndWait()

		if ex.hasConflict.Load() {
			for _, tx := range batch {
				if tx.flagConflict {
					ex.Fallback(tx)
					ex.stats.JournalExecute()
					latency := int64(time.Since(tx.startTime) / time.Microsecond)
					ex.stats.JournalCommit(latency)
					ex.stats.JournalRollback(tx.CountOverheads())
				}
			}
		}

		ex.barrier.ArriveAndWait()

		for _, tx := range batch {
			ex.CleanLockTable(tx)
		}
	}
}
