package harmony

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
	ariaAbortTxs []map[int]*HarmonyTransaction // block -> aborted txs in this block
)

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) [][]float64 {
	txGenerator := common.NewTxGenerator(janusConfig.AllBlocksTxSum, janusConfig.BlockSize)
	blocks := txGenerator.GenerateWorkload(blockTxs) // 生成区块
	//fmt.Printf("Blocks num: %d, Blocks size: %d\n", len(blocks), len(blocks[0].Txs))
	if tools.TraceAbort {
		ariaAbortTxs = make([]map[int]*HarmonyTransaction, len(blockTxs))
		for i, _ := range blockTxs {
			ariaAbortTxs[i] = make(map[int]*HarmonyTransaction)
		}
	}

	startTime := time.Now()
	static := common.NewStatistics()
	harmonyInstance := NewHarmony(blocks, static, janusConfig.AllThreadNum, 4, true)
	harmonyInstance.Start(levm)
	elapsed := time.Since(startTime)

	fmt.Printf("CommitCount= %d \n", harmonyInstance.Statistics.CommitCount.Load())
	fmt.Printf("交易实际被执行总次数 %d \n", harmonyInstance.Statistics.ExecCount.Load())
	tps := float64(harmonyInstance.Statistics.CommitCount.Load()) / (elapsed.Seconds())
	fmt.Printf("交易处理吞吐(TPS)= %f \n", tps)
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
	return [][]float64{[]float64{tps}, []float64{elapsed.Seconds()}}
}
