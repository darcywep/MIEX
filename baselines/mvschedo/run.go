package mvschedo

import (
	"Janus/baselines/common"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"fmt"
	"sort"
	"sync"
	"time"
)

const smfSeedBase int64 = 20270608

type MVSchedO struct {
	Statistics      *common.Statistics
	blocks          []*common.Block
	numThreads      int
	tablePartitions int
	sampleSize      int
}

func NewMVSchedO(blocks []*common.Block, statistics *common.Statistics, numThreads int, tablePartitions int, sampleSize int) *MVSchedO {
	return &MVSchedO{
		Statistics:      statistics,
		blocks:          blocks,
		numThreads:      normalizedWorkerCount(numThreads),
		tablePartitions: tablePartitions,
		sampleSize:      sampleSize,
	}
}

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) [][]float64 {
	fmt.Println("=== Run MVSchedO ===")

	start := time.Now()
	txGenerator := common.NewTxGenerator(janusConfig.AllBlocksTxSum, janusConfig.BlockSize)
	blocks := txGenerator.GenerateWorkload(blockTxs)

	stats := common.NewStatistics()
	mvschedo := NewMVSchedO(blocks, stats, janusConfig.AllThreadNum, 4, defaultSMFSampleSize)
	mvschedo.Start(levm)

	elapsed := time.Since(start)
	committed := stats.CommitCount.Load()
	tps := 0.0
	if elapsed.Seconds() > 0 {
		tps = float64(committed) / elapsed.Seconds()
	}

	fmt.Printf("CommitCount= %d \n", committed)
	fmt.Printf("交易实际被执行总次数 %d \n", stats.ExecCount.Load())
	fmt.Printf("交易处理吞吐(TPS)= %f \n", tps)
	fmt.Printf("MVSchedO Execution Time:     %-22v \n", elapsed)

	return [][]float64{{tps}, {elapsed.Seconds()}}
}

func (m *MVSchedO) Start(levm *lvm.LEVM) {
	for blockID, block := range m.blocks {
		txs := m.preExecuteBlock(block, levm)
		scheduler := NewSMFScheduler(m.sampleSize, smfSeedBase+int64(blockID))
		scheduled := scheduler.Schedule(txs)
		m.executeScheduledBlock(scheduled, levm)
		m.Statistics.JournalBlock()
	}
}

func (m *MVSchedO) preExecuteBlock(block *common.Block, levm *lvm.LEVM) []*MVSchedOTransaction {
	basicTxs := block.GetTxs()
	txs := make([]*MVSchedOTransaction, len(basicTxs))
	for idx, basicTx := range basicTxs {
		txs[idx] = NewMVSchedOTransaction(basicTx)
	}

	jobs := make(chan *MVSchedOTransaction)
	var wg sync.WaitGroup
	for workerID := 0; workerID < m.numThreads; workerID++ {
		wg.Add(1)
		workerEVM := levm.Copy()
		go func() {
			defer wg.Done()
			for tx := range jobs {
				executeEthTransaction(tx, workerEVM)
				tools.FillStringReadWriteSet(tx.Inner.EthTx, tx.Inner.Vertex.ReadKeys, tx.Inner.Vertex.WriteKeys)
				tx.RefreshReadWriteSet()
				m.Statistics.AddExecCount()
			}
		}()
	}

	for _, tx := range txs {
		jobs <- tx
	}
	close(jobs)
	wg.Wait()

	return txs
}

func (m *MVSchedO) executeScheduledBlock(txs []*MVSchedOTransaction, levm *lvm.LEVM) {
	table := NewMVCCTable()
	queues := NewScheduleQueues(txs)
	workerBatches := make([][]*MVSchedOTransaction, m.numThreads)

	for idx, tx := range txs {
		workerID := idx % m.numThreads
		workerBatches[workerID] = append(workerBatches[workerID], tx)
	}

	var wg sync.WaitGroup
	workerEVMs := make([]*lvm.LEVM, m.numThreads)
	abortedTxs := make([]*MVSchedOTransaction, 0)
	var abortMu sync.Mutex

	for workerID := 0; workerID < m.numThreads; workerID++ {
		workerEVMs[workerID] = levm.Copy()
		batch := workerBatches[workerID]
		wg.Add(1)
		go func(workerEVM *lvm.LEVM, workerTxs []*MVSchedOTransaction) {
			defer wg.Done()
			for _, tx := range workerTxs {
				if m.processTransaction(tx, table, queues, workerEVM) {
					continue
				}
				abortMu.Lock()
				abortedTxs = append(abortedTxs, tx)
				abortMu.Unlock()
			}
		}(workerEVMs[workerID], batch)
	}

	wg.Wait()

	for _, workerEVM := range workerEVMs {
		workerEVM.AllDB().StateDB.FlushDirtyToNewStateDB(levm.AllDB().StateDB)
	}

	m.serialFallback(abortedTxs, levm)
}

func (m *MVSchedO) processTransaction(tx *MVSchedOTransaction, table *MVCCTable, queues *ScheduleQueues, levm *lvm.LEVM) bool {
	tx.StartTime = time.Now()

	for _, op := range tx.Ops {
		queues.WaitTurn(tx, op)

		ok := true
		if op.Type == ReadOperation {
			tx.LocalGet[op.Key] = table.Read(tx, op.Key)
		} else {
			ok = table.Write(tx, op.Key, tx.LocalPut[op.Key])
		}

		queues.MarkDone(tx, op)
		if ok {
			continue
		}

		queues.FreeTransaction(tx)
		table.Abort(tx)
		m.Statistics.AddRollbackCount()
		return false
	}

	queues.FreeTransaction(tx)
	if !table.WaitDependencies(tx) {
		table.Abort(tx)
		m.Statistics.AddRollbackCount()
		return false
	}

	executeEthTransaction(tx, levm)
	m.Statistics.AddExecCount()
	table.MarkCommitted(tx)
	m.Statistics.JournalCommit(uint32(time.Since(tx.StartTime).Microseconds()))
	return true
}

func (m *MVSchedO) serialFallback(txs []*MVSchedOTransaction, levm *lvm.LEVM) {
	if len(txs) == 0 {
		return
	}

	sort.Slice(txs, func(i, j int) bool {
		return txs[i].Timestamp < txs[j].Timestamp
	})

	for _, tx := range txs {
		tx.StartTime = time.Now()
		executeEthTransaction(tx, levm)
		m.Statistics.AddExecCount()
		tx.MarkCommitted()
		m.Statistics.JournalCommit(uint32(time.Since(tx.StartTime).Microseconds()))
	}
}

func normalizedWorkerCount(numThreads int) int {
	if numThreads <= 0 {
		return 1
	}
	return numThreads
}
