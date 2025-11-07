package aria

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// -----------------------------
// Aria: 主要协议
// -----------------------------
type Aria struct {
	statistics    *Statistics
	blocks        []*Block
	table         *AriaTable
	lockTable     *AriaLockTable
	enableReorder bool
	numThreads    int
	confirmExit   int32
	stopFlag      atomic.Bool
	barrier       *CyclicBarrier
	counter       int32
	hasConflict   atomic.Bool
	workers       []sync.WaitGroup
}

func NewAria(blocks []*Block, stats *Statistics, numThreads int, tablePartitions int, enableReorder bool) *Aria {
	aria := &Aria{
		statistics:    stats,
		blocks:        blocks,
		table:         NewAriaTable(tablePartitions),
		lockTable:     NewAriaLockTable(tablePartitions),
		enableReorder: enableReorder,
		numThreads:    numThreads,
	}
	aria.barrier = NewCyclicBarrier(numThreads, func() {
		log.Println("batch complete")
	})
	return aria
}

func (a *Aria) Start() {
	log.Println("aria start")
	// 分割区块为批次
	type threadBatch [][]*AriaTransaction
	allThreadBatches := make([]threadBatch, a.numThreads)

	for i := 0; i < len(a.blocks); i++ {
		block := a.blocks[i]
		txs := block.getTxs()
		txPerThread := 1
		index := 0
		batchID := uint64(i + 1)
		batch := make([][]*AriaTransaction, a.numThreads)
		for j := 0; j < len(txs); j += txPerThread {
			batchIdx := index % a.numThreads
			for k := 0; k < txPerThread && j+k < len(txs); k++ {
				tx := txs[j+k]
				txid := uint64(tx.HyperId)
				inner := *tx
				atx := NewAriaTransaction(inner, txid, batchID)
				batch[batchIdx] = append(batch[batchIdx], atx)
			}
			index++
		}
		// 分配批次到每个线程
		for t := 0; t < a.numThreads; t++ {
			allThreadBatches[t] = append(allThreadBatches[t], batch[t])
		}
		a.statistics.JournalBlock()
	}

	// 启动 worker 协程
	for i := 0; i < a.numThreads; i++ {
		threadBatches := allThreadBatches[i]
		var wg sync.WaitGroup
		wg.Add(1)
		go func(workerID int, batches [][]*AriaTransaction, wg *sync.WaitGroup) {
			defer wg.Done()
			ex := NewAriaExecutor(a, workerID, batches)
			ex.Run()
		}(i, threadBatches, &wg)
		a.workers = append(a.workers, wg)
	}
}

func (a *Aria) Stop() {
	a.stopFlag.Store(true)
	// 等待所有工作协程
	for i := 0; i < len(a.workers); i++ {
		a.workers[i].Wait()
	}
	log.Println("aria stop")
}
