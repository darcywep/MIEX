package thunderbolt

import (
	"Janus/baselines/common"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Thunderbolt struct {
	statistics *common.Statistics
	blocks     []*common.Block
	workers    int
	validators int
	levm       *lvm.LEVM
}

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) [][]float64 {
	fmt.Println("=== Run Thunderbolt ===")

	start := time.Now()
	txGenerator := common.NewTxGenerator(janusConfig.AllBlocksTxSum, janusConfig.BlockSize)
	blocks := txGenerator.GenerateWorkload(blockTxs)

	stats := common.NewStatistics()
	thunderbolt := NewThunderbolt(blocks, stats, janusConfig.AllThreadNum, levm)
	fmt.Printf("Thunderbolt CE executors: %d \n", thunderbolt.workers)
	fmt.Printf("Thunderbolt validators: %d \n", thunderbolt.validators)
	thunderbolt.Start()

	elapsed := time.Since(start)
	committed := stats.CommitCount.Load()
	tps := 0.0
	if elapsed.Seconds() > 0 {
		tps = float64(committed) / elapsed.Seconds()
	}

	fmt.Printf("CommitCount= %d \n", committed)
	fmt.Printf("交易实际被执行总次数 %d \n", stats.ExecCount.Load())
	fmt.Printf("Thunderbolt validation failures= %d \n", stats.RollbackCount.Load())
	fmt.Printf("交易处理吞吐(TPS)= %f \n", tps)
	fmt.Printf("Thunderbolt Execution Time:     %-22v \n", elapsed)

	return [][]float64{{tps}, {elapsed.Seconds()}}
}

func NewThunderbolt(blocks []*common.Block, statistics *common.Statistics, workerCount int, levm *lvm.LEVM) *Thunderbolt {
	workers := normalizeWorkerCount(workerCount)
	return &Thunderbolt{
		statistics: statistics,
		blocks:     blocks,
		workers:    workers,
		validators: workers,
		levm:       levm,
	}
}

func (t *Thunderbolt) Start() {
	for blockID, block := range t.blocks {
		blockStart := time.Now()

		preplayStart := time.Now()
		txs := t.preplayBlock(block)
		preplayElapsed := time.Since(preplayStart)

		planStart := time.Now()
		plan := buildThunderboltPlan(txs)
		planElapsed := time.Since(planStart)

		validateStart := time.Now()
		if err := t.validatePlan(plan); err != nil {
			t.statistics.AddRollbackCount()
			panic(err)
		}
		validateElapsed := time.Since(validateStart)

		commitStart := time.Now()
		t.commitPlan(plan)
		commitElapsed := time.Since(commitStart)
		t.statistics.JournalBlock()

		if shouldPrintBlockProgress(blockID, len(t.blocks)) {
			fmt.Printf("[Thunderbolt] block %d/%d txs=%d preplay=%v schedule=%v validate=%v commit=%v total=%v commits=%d\n",
				blockID+1,
				len(t.blocks),
				len(txs),
				preplayElapsed,
				planElapsed,
				validateElapsed,
				commitElapsed,
				time.Since(blockStart),
				t.statistics.CommitCount.Load(),
			)
		}
	}
}

func (t *Thunderbolt) preplayBlock(block *common.Block) []*thunderboltTransaction {
	basicTxs := block.GetTxs()
	txs := make([]*thunderboltTransaction, len(basicTxs))
	for idx, basicTx := range basicTxs {
		txs[idx] = newThunderboltTransaction(basicTx)
	}
	if len(txs) == 0 {
		return txs
	}

	jobs := make(chan *thunderboltTransaction)
	var wg sync.WaitGroup
	var completionOrder atomic.Int64
	for workerID := 0; workerID < t.workers; workerID++ {
		wg.Add(1)
		workerEVM := t.copyMasterEVM()
		if workerEVM != nil {
			workerEVM.SetEVMWorkerID(workerID)
		}
		go func(localEVM *lvm.LEVM) {
			defer wg.Done()
			for tx := range jobs {
				tx.startTime = time.Now()
				executeThunderboltEthTransaction(tx, localEVM)
				if tx.inner != nil && tx.inner.Vertex != nil {
					tools.FillStringReadWriteSet(tx.inner.EthTx, tx.inner.Vertex.ReadKeys, tx.inner.Vertex.WriteKeys)
				}
				tx.refreshReadWriteSet()
				tx.preplayOrder = int(completionOrder.Add(1))
				t.statistics.AddExecCount()
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

func (t *Thunderbolt) validatePlan(plan *thunderboltExecutionPlan) error {
	if plan == nil || len(plan.allTxs) == 0 {
		return nil
	}

	scheduler := newThunderboltValidationScheduler(plan)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup
	for workerID := 0; workerID < t.validators; workerID++ {
		wg.Add(1)
		workerEVM := t.copyMasterEVM()
		if workerEVM != nil {
			workerEVM.SetEVMWorkerID(workerID)
		}
		go func(localEVM *lvm.LEVM) {
			defer wg.Done()
			for {
				tx := scheduler.next()
				if tx == nil {
					return
				}
				if err := validateThunderboltTransaction(tx, localEVM); err != nil {
					select {
					case errChan <- err:
					default:
					}
					scheduler.fail()
					return
				}
				t.statistics.AddExecCount()
				scheduler.complete(plan, tx)
			}
		}(workerEVM)
	}
	wg.Wait()

	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

func (t *Thunderbolt) commitPlan(plan *thunderboltExecutionPlan) {
	if plan == nil {
		return
	}
	for _, tx := range plan.order {
		latency := uint32(0)
		if !tx.startTime.IsZero() {
			latency = uint32(time.Since(tx.startTime).Microseconds())
		}
		t.statistics.JournalCommit(latency)
	}
}

func (t *Thunderbolt) copyMasterEVM() *lvm.LEVM {
	if t.levm == nil {
		return nil
	}
	return t.levm.Copy()
}

func normalizeWorkerCount(workerCount int) int {
	if workerCount <= 0 {
		return 1
	}
	return workerCount
}

func shouldPrintBlockProgress(blockID, blockCount int) bool {
	if blockCount <= 20 {
		return true
	}
	if blockID == 0 || blockID+1 == blockCount {
		return true
	}
	return (blockID+1)%100 == 0
}
