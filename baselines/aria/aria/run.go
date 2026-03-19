package aria

import (
	"Janus/baselines/common"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/januscore/janus"
	"Janus/tools"
	"fmt"
	"time"
)

var (
	ariaAbortTxs []map[int]*AriaTransaction // block -> aborted txs in this block
)

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) [][]float64 {
	start := time.Now()
	txGenerator := common.NewTxGenerator(janusConfig.AllBlocksTxSum, janusConfig.BlockSize) // TX_NUM = 2000, BLOCK_SIZE = 1000
	
	if tools.TraceAbort {
		ariaAbortTxs = make([]map[int]*AriaTransaction, 0, len(blockTxs))
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
		abortSum := 0
		var cost float64 = 0.0
		for blockID, v := range ariaAbortTxs {
			abortSum += len(v)
			for index, _ := range v {
				cost += janus.AllJanusTransactions[blockID][index].ExecutionCost
			}
		}
		fmt.Printf("Number of abort transaction:         %-22d\n", abortSum)
		fmt.Printf("Cost of abort transactions:         %-22.2f\n", cost)
	}
	fmt.Println("Aria TPS: ", tps)

	defer aria.EvmClose()
	return [][]float64{[]float64{tps}, []float64{latency}}
}
