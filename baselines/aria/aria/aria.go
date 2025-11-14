package aria

import (
	"Janus/baselines/common"
	lvm "Janus/core/evm"
	"Janus/ethereum/database"
	"Janus/tools"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"math/big"
)

type Aria struct {
	levm          *lvm.LEVM
	statistics    *common.Statistics
	blocks        []*common.Block
	table         *AriaTable
	lockTable     *AriaLockTable
	enableReorder bool
	numThreads    int
	confirmExit   atomic.Int64
	stopFlag      atomic.Bool
	barrier       *Barrier
	counter       atomic.Int64
	hasConflict   atomic.Bool
	workers       []*sync.WaitGroup
	levms         []*lvm.LEVM
}

func NewAria(blocks []*common.Block, stats *common.Statistics, numThreads int, tablePartitions int, enableReorder bool) *Aria {
	// Step 3: 模拟执行
	levm := lvm.New(database.SmallBankStateDBConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())
	aria := &Aria{
		levm:          levm,
		statistics:    stats,
		blocks:        blocks,
		table:         NewAriaTable(tablePartitions),
		lockTable:     NewAriaLockTable(tablePartitions),
		enableReorder: enableReorder,
		numThreads:    numThreads,
		levms:         make([]*lvm.LEVM, 0),
	}
	aria.barrier = NewBarrier(numThreads)
	return aria
}
func (a *Aria) EvmClose() {
	defer a.levm.AllDB().Close()
}

func (a *Aria) Statistics() *common.Statistics {
	return a.statistics
}

func (a *Aria) Start() {
	log.Println("Aria start")
	// 分割区块为批次
	type threadBatch [][]*AriaTransaction
	allThreadBatches := make([]threadBatch, a.numThreads)

	for i := 0; i < len(a.blocks); i++ {
		block := a.blocks[i]
		txs := block.GetTxs()
		txPerThread := 1
		index := 0
		batchID := uint64(i + 1)
		batch := make([][]*AriaTransaction, a.numThreads)
		ethTxIndex := 0

		for j := 0; j < len(txs); j += txPerThread {
			batchIdx := index % a.numThreads
			for k := 0; k < txPerThread && j+k < len(txs); k++ {
				tx := txs[j+k]
				txid := uint64(tx.Txid)
				inner := *tx
				atx := NewAriaTransaction(inner, txid, batchID)
				ethTxIndex++ // 移动到下一个以太坊交易
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
		a.levms = append(a.levms, a.levm.Copy())
		var wg sync.WaitGroup
		wg.Add(1)
		go func(workerID int, batches [][]*AriaTransaction, wg *sync.WaitGroup, levm *lvm.LEVM) {
			defer wg.Done()
			ex := NewAriaExecutor(a, levm, workerID, batches)
			ex.Run()
		}(i, threadBatches, &wg, a.levms[len(a.levms)-1])
		a.workers = append(a.workers, &wg)
	}
}

func (a *Aria) Stop() {
	//a.stopFlag.Store(true)
	// 等待所有工作协程
	for i := 0; i < len(a.workers); i++ {
		a.workers[i].Wait()
		a.levms[i].AllDB().StateDB.FlushDirtyToNewStateDB(a.levm.AllDB().StateDB)
	}
	root, err := a.levm.AllDB().StateDB.Commit(uint64(0), true, true)
	if err != nil {
		fmt.Println("StateDB.Commit", err)
	}
	err = a.levm.AllDB().StateDB.Database().TrieDB().Commit(root, false)
	if err != nil {
		fmt.Println("TrieDB().Commit(root, false)", err)
	}
	log.Println("aria stop")
}
