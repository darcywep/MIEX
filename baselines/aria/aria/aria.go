package aria

import (
	"Janus/tools"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"Janus/baselines/common"
	lvm "Janus/core/evm"
)

// Aria 顶层调度器 / 控制器
type Aria struct {
	levm            *lvm.LEVM
	statistics      *common.Statistics
	blocks          []*common.Block
	table           *AriaTable
	enableReorder   bool
	numThreads      int
	tablePartitions int

	workersWG sync.WaitGroup

	// per-worker channels：每次发一笔 tx（或 nil）
	jobChans    []chan *AriaTransaction
	resultChans []chan *AriaTransaction

	// each worker 的 levm
	levms []*lvm.LEVM

	// 每个block内使用的 barrier（在启动前创建并传给 worker）
	barrier *Barrier

	stopFlag atomic.Bool
}

func NewAria(blocks []*common.Block, stats *common.Statistics, numThreads int, tablePartitions int, enableReorder bool, levm *lvm.LEVM) *Aria {
	aria := &Aria{
		levm:            levm.Copy(),
		statistics:      stats,
		blocks:          blocks,
		tablePartitions: tablePartitions,
		table:           NewAriaTable(tablePartitions),
		enableReorder:   enableReorder,
		numThreads:      numThreads - 1, // 留一个给主控
		levms:           make([]*lvm.LEVM, 0),
	}
	// barrier 在 Start 时初始化并传递给 worker（也可以在 NewAria 初始化）
	aria.barrier = NewBarrier(aria.numThreads)
	return aria
}

func (a *Aria) EvmClose() {
	defer a.levm.AllDB().Close()
}

func (a *Aria) Statistics() *common.Statistics {
	return a.statistics
}

// Start 使用新的 per-block、按列同步的执行模型运行 Aria
func (a *Aria) Start() {
	fmt.Println("================ Aria start =================")
	// perThreadBatch 表示一个区块里所有线程的 batch， 下标为 threadID
	type perThreadBatch [][]*AriaTransaction

	// blockBatches[blockID][threadID] = []*AriaTransaction
	var blockBatches []perThreadBatch = make([]perThreadBatch, len(a.blocks))

	for blockID := 0; blockID < len(a.blocks); blockID++ {
		block := a.blocks[blockID]
		txs := block.GetTxs()

		// 初始化当前区块：每个线程一个 batch
		perThreadBatches := make(perThreadBatch, a.numThreads)

		batchID := uint64(blockID + 1)
		txPerThread := 1 // 目前为 round-robin

		for txIndex := 0; txIndex < len(txs); txIndex += txPerThread {
			// 将交易分配给线程
			threadID := txIndex % a.numThreads

			for k := 0; k < txPerThread && txIndex+k < len(txs); k++ {
				tx := txs[txIndex+k]

				inner := *tx
				atx := NewAriaTransaction(inner, uint64(tx.Txid), batchID, tx.OriginalBlockID, tx.OriginalTxID)

				perThreadBatches[threadID] = append(perThreadBatches[threadID], atx)
			}
		}
		blockBatches[batchID-1] = perThreadBatches
		a.statistics.JournalBlock()
	}
	for blockID := 0; blockID < len(a.blocks); blockID++ {
		a.table = NewAriaTable(a.tablePartitions)
		a.processBlock(blockBatches[blockID], blockID)
	}
	log.Println("==================== aria stop ======================")
}

func (a *Aria) processBlock(perThreadBatch [][]*AriaTransaction, blockID int) {
	// ---- 准备 channels 与 worker levm ----
	a.jobChans = make([]chan *AriaTransaction, a.numThreads)
	a.resultChans = make([]chan *AriaTransaction, a.numThreads)
	for i := 0; i < a.numThreads; i++ {
		a.jobChans[i] = make(chan *AriaTransaction)
		a.resultChans[i] = make(chan *AriaTransaction)
		a.levms = append(a.levms, a.levm.Copy())
	}

	// ---- 启动 worker goroutines ----
	a.workersWG.Add(a.numThreads)
	for i := 0; i < a.numThreads; i++ {
		go func(workerID int, jobs <-chan *AriaTransaction, results chan<- *AriaTransaction, levm *lvm.LEVM) {
			defer a.workersWG.Done()
			//fmt.Println("worker run", workerID)
			ex := NewAriaExecutor(a, levm, workerID)
			// worker 循环：每次 receive 一个 tx（或 nil），执行 Execute/Reserve，然后 barrier.Wait()，再 Verify/Commit，并把 abort（或 nil）通过 results 发回控制器
			for tx := range jobs {
				// 当主控关闭 channel 时退出
				// 对于每次发送（代表同一列的交易），worker 要在本地执行 + reserve，然后 barrier，同步后再 verify/commit
				ex.ProcessOneTx(tx)
				// ex.ProcessOneTx 会在内部发送一个结果到 results chan（nil 或 tx）
				// 这里不额外做事
			}
			// 关闭 results 由 controller 控制，这里不做 close
		}(i, a.jobChans[i], a.resultChans[i], a.levms[i])
	}

	// 对每一列进行：下发 → barrier（在 worker 内部）→ 收集结果
	batchID := uint64(0)
	for {
		// 1) 向每个 worker 发送该列的交易（不存在发 nil）
		nilNumber := 0
		for t := 0; t < a.numThreads; t++ {
			var txToSend *AriaTransaction
			if len(perThreadBatch[t]) > 0 {
				txToSend = perThreadBatch[t][0]
				txToSend.BatchID = batchID
				perThreadBatch[t] = perThreadBatch[t][1:]
			} else {
				txToSend = nil
				nilNumber++
			}
			a.jobChans[t] <- txToSend
		}
		batchID++

		// 2) 收集来自每个 worker 的 Verify 结果（若被 abort，会返回该 tx；否则返回 nil）
		//    并把 aborted 插入到每一列的开头（如果存在下一区块），或扩展 blockBatches 以容纳新的重试批次
		for t := 0; t < a.numThreads; t++ {
			res := <-a.resultChans[t]
			if res == nil {
				continue
			}

			// aborted tx：插回下一列的开头，等待重试
			perThreadBatch[t] = append([]*AriaTransaction{res}, perThreadBatch[t]...)
			if tools.TraceAbort {
				tools.TraceAbortMutex.Lock()
				ariaAbortTxs[res.OriginalBlockID][res.OriginalTxID] = res
				tools.TraceAbortMutex.Unlock()
			}
		}
		if nilNumber == a.numThreads {
			break
		}
	}

	// 所有交易处理完毕 -> 关闭 job channels，等待 workers 退出
	for t := 0; t < a.numThreads; t++ {
		close(a.jobChans[t])
	}
	a.workersWG.Wait()

	// flush worker levm state back to master levm
	for i := 0; i < len(a.levms); i++ {
		a.levms[i].AllDB().StateDB.FlushDirtyToNewStateDB(a.levm.AllDB().StateDB)
	}
	//if tools.TraceAbort {
	//	tools.TraceAbortMutex.Lock()
	//	for _, tx := range abortTxs {
	//		ariaAbortTxs[tx.OriginalBlockID][tx.OriginalTxID] = tx
	//	}
	//	tools.TraceAbortMutex.Unlock()
	//}
}

func (a *Aria) Stop() {
	a.stopFlag.Store(true)
	// 可扩展：向 jobChans 发送 nil 或关闭 channel 来提前结束
}
