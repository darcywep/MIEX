package aria

import (
	"Janus/baselines/common"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"fmt"
	"time"
)

var (
	ariaAbortTxs   []map[int]*AriaTransaction // block -> aborted txs in this block
	ariaAbortCount int
	ariaAbortCost  float64
)

func resetAriaAbortStats(blockNum int) {
	ariaAbortCount = 0
	ariaAbortCost = 0
	ariaAbortTxs = make([]map[int]*AriaTransaction, blockNum)
	for i := range ariaAbortTxs {
		ariaAbortTxs[i] = make(map[int]*AriaTransaction)
	}
}

func recordAriaAbort(tx *AriaTransaction) {
	if tx == nil {
		return
	}
	tools.TraceAbortMutex.Lock()
	defer tools.TraceAbortMutex.Unlock()
	ariaAbortCount++
	ariaAbortCost += tx.ExecutionCost
	ariaAbortTxs[tx.OriginalBlockID][tx.OriginalTxID] = tx
}

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) [][]float64 {
	start := time.Now()
	txGenerator := common.NewTxGenerator(janusConfig.AllBlocksTxSum, janusConfig.BlockSize) // TX_NUM = 2000, BLOCK_SIZE = 1000

	if tools.TraceAbort {
		resetAriaAbortStats(len(blockTxs))
	}
	blocks := txGenerator.GenerateWorkload(blockTxs) // 生成区块
	//fmt.Printf("Blocks num: %d\n", len(blocks))
	//fmt.Printf("Blocks size: %d\n", len(blocks[0].Txs))

	static := common.NewStatistics()
	aria := NewAria(blocks, static, janusConfig.AllThreadNum, 4, true, levm)
	//tools.CatStorageState = true
	aria.Start()
	aria.Stop()
	latency := time.Since(start).Seconds()
	fmt.Println("CommitCount=", aria.Statistics().CommitCount.Load())
	tps := float64(aria.Statistics().CommitCount.Load()) / latency
	if tools.TraceAbort {
		tools.TraceAbortMutex.Lock()
		abortSum := ariaAbortCount
		cost := ariaAbortCost
		tools.TraceAbortMutex.Unlock()
		fmt.Printf("Number of abort transaction:         %-22d\n", abortSum)
		fmt.Printf("Cost of abort transactions:         %-22.2f\n", cost)
	}
	fmt.Println("Aria TPS: ", tps)

	defer aria.EvmClose()
	return [][]float64{[]float64{tps}, []float64{latency}}
}
