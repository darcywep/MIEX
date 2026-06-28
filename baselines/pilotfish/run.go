package pilotfish

import (
	"Janus/baselines/common"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"fmt"
	"sync"
	"time"
)

type Pilotfish struct {
	statistics  *common.Statistics
	blocks      []*common.Block
	workerCount int
	levm        *lvm.LEVM
}

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) [][]float64 {
	fmt.Println("=== Run Pilotfish ===")

	start := time.Now()
	txGenerator := common.NewTxGenerator(janusConfig.AllBlocksTxSum, janusConfig.BlockSize)
	blocks := txGenerator.GenerateWorkload(blockTxs)

	stats := common.NewStatistics()
	pilotfish := NewPilotfish(blocks, stats, janusConfig.AllThreadNum, levm)
	fmt.Printf("Pilotfish simulated execution workers: %d \n", pilotfish.workerCount)
	pilotfish.Start()

	elapsed := time.Since(start)
	committed := stats.CommitCount.Load()
	tps := 0.0
	if elapsed.Seconds() > 0 {
		tps = float64(committed) / elapsed.Seconds()
	}

	fmt.Printf("CommitCount= %d \n", committed)
	fmt.Printf("交易实际被执行总次数 %d \n", stats.ExecCount.Load())
	fmt.Printf("Pilotfish rollback count= %d \n", stats.RollbackCount.Load())
	fmt.Printf("Pilotfish TPS= %f \n", tps)
	fmt.Printf("Pilotfish Execution Time:     %-22v \n", elapsed)

	return [][]float64{{tps}, {elapsed.Seconds()}}
}

func NewPilotfish(blocks []*common.Block, statistics *common.Statistics, workerCount int, levm *lvm.LEVM) *Pilotfish {
	return &Pilotfish{
		statistics:  statistics,
		blocks:      blocks,
		workerCount: normalizeWorkerCount(workerCount),
		levm:        levm,
	}
}

func (p *Pilotfish) Start() {
	for blockID, block := range p.blocks {
		blockStart := time.Now()
		preStart := time.Now()
		txs := p.preExecuteBlock(block)
		preElapsed := time.Since(preStart)

		planStart := time.Now()
		plan := buildPilotfishPlan(txs, p.workerCount)
		planElapsed := time.Since(planStart)

		executeStart := time.Now()
		p.executePlan(plan)
		executeElapsed := time.Since(executeStart)
		p.statistics.JournalBlock()

		if shouldPrintBlockProgress(blockID, len(p.blocks)) {
			fmt.Printf("[Pilotfish] block %d/%d txs=%d pre_execute=%v schedule=%v execute=%v total=%v commits=%d\n",
				blockID+1,
				len(p.blocks),
				len(txs),
				preElapsed,
				planElapsed,
				executeElapsed,
				time.Since(blockStart),
				p.statistics.CommitCount.Load(),
			)
		}
	}
}

func (p *Pilotfish) preExecuteBlock(block *common.Block) []*pilotfishTransaction {
	basicTxs := block.GetTxs()
	txs := make([]*pilotfishTransaction, len(basicTxs))
	for idx, basicTx := range basicTxs {
		txs[idx] = newPilotfishTransaction(basicTx)
	}
	if len(txs) == 0 {
		return txs
	}

	jobs := make(chan *pilotfishTransaction)
	var wg sync.WaitGroup
	for workerID := 0; workerID < p.workerCount; workerID++ {
		wg.Add(1)
		var workerEVM *lvm.LEVM
		if p.levm != nil {
			workerEVM = p.levm.Copy()
			workerEVM.SetEVMWorkerID(workerID)
		}
		go func(localEVM *lvm.LEVM) {
			defer wg.Done()
			for tx := range jobs {
				executePilotfishEthTransaction(tx, localEVM)
				if tx.inner != nil && tx.inner.Vertex != nil {
					tools.FillStringReadWriteSet(tx.inner.EthTx, tx.inner.Vertex.ReadKeys, tx.inner.Vertex.WriteKeys)
				}
				tx.refreshReadWriteSet()
				p.statistics.AddExecCount()
			}
		}(workerEVM)
	}

	for _, tx := range txs {
		jobs <- tx
	}
	close(jobs)
	wg.Wait()

	return txs
}

func (p *Pilotfish) executePlan(plan *pilotfishExecutionPlan) {
	if plan == nil || plan.remaining == 0 {
		return
	}

	workerEVMs := make([]*lvm.LEVM, p.workerCount)
	if p.levm != nil {
		for workerID := 0; workerID < p.workerCount; workerID++ {
			workerEVMs[workerID] = p.levm.Copy()
			workerEVMs[workerID].SetEVMWorkerID(workerID)
		}
	}

	scheduler := newPilotfishScheduler(plan)
	var wg sync.WaitGroup
	for workerID := 0; workerID < p.workerCount; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				tx := scheduler.next(id)
				if tx == nil {
					return
				}
				tx.startTime = time.Now()
				executePilotfishEthTransaction(tx, workerEVMs[id])
				p.statistics.AddExecCount()
				p.statistics.JournalCommit(uint32(time.Since(tx.startTime).Microseconds()))
				scheduler.complete(tx)
			}
		}(workerID)
	}
	wg.Wait()

	if p.levm != nil {
		for _, workerEVM := range workerEVMs {
			if workerEVM == nil {
				continue
			}
			workerEVM.AllDB().StateDB.FlushDirtyToNewStateDB(p.levm.AllDB().StateDB)
		}
	}
}

func shouldPrintBlockProgress(blockID int, totalBlocks int) bool {
	if totalBlocks <= 20 {
		return true
	}
	if blockID < 3 || blockID == totalBlocks-1 {
		return true
	}
	return (blockID+1)%1000 == 0
}
