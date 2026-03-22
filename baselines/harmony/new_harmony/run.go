package newHarmony

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
	ariaAbortTxs []map[int]*HarmonyTransaction
)

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) [][]float64 {
	if tools.TraceAbort {
		blockTxs = make([]types.Transactions, 0)
		for _, jtxs := range janus.JanusTxsForOneBatchAsOneBlock {
			txs := make(types.Transactions, len(jtxs))
			for i, jtx := range jtxs {
				txs[i] = jtx.Tx
			}
			blockTxs = append(blockTxs, txs)
		}
	}
	txGenerator := common.NewTxGenerator(janusConfig.AllBlocksTxSum, janusConfig.BlockSize)
	var blocks []*common.Block
	if tools.TraceAbort {
		blocks = txGenerator.GenerateWorkloadForHarmonyAbort(blockTxs)
	} else {
		blocks = txGenerator.GenerateWorkload(blockTxs)
	}

	if tools.TraceAbort {
		ariaAbortTxs = make([]map[int]*HarmonyTransaction, len(blockTxs))
		for i := range blockTxs {
			ariaAbortTxs[i] = make(map[int]*HarmonyTransaction)
		}
	}

	startTime := time.Now()
	static := common.NewStatistics()
	harmonyInstance := NewHarmony(blocks, static, janusConfig.AllThreadNum, 1, false)
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
			for index := range v {
				cost += janus.JanusTxsForOneBatchAsOneBlock[blockID][index].ExecutionCost
			}
		}
		fmt.Printf("Number of abort transaction:         %-22d\n", abortSum)
		fmt.Printf("Cost of abort transactions:         %-22.2f\n", cost)
	}
	return [][]float64{{tps}, {elapsed.Seconds()}}
}
