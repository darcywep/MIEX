package optme

import (
	"Janus/baselines/common"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/januscore/janus"
	"fmt"
	"time"

	"Janus/tools"
)

var (
	ariaAbortTxs []map[int]*OptmeTransaction // block -> aborted txs in this block
)

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) [][]float64 {
	fmt.Println("optme start")

	if tools.TraceAbort {
		ariaAbortTxs = make([]map[int]*OptmeTransaction, len(blockTxs))
		for i, _ := range blockTxs {
			ariaAbortTxs[i] = make(map[int]*OptmeTransaction)
		}
	}

	startTime := time.Now()
	txGenerator := common.NewTxGenerator(janusConfig.AllBlocksTxSum, janusConfig.BlockSize)

	blocks := txGenerator.GenerateWorkload(blockTxs) // 生成区块
	fmt.Printf("Blocks num: %d\n", len(blocks))
	fmt.Printf("Blocks size: %d\n", len(blocks[0].Txs))

	static := common.NewStatistics()
	optmeInstance := NewOptME(blocks, static, janusConfig.AllThreadNum, 4, true, levm)

	optmeInstance.Start()
	elapsed := time.Since(startTime)

	fmt.Printf("被执行的交易数目 %d \n", optmeInstance.Statistics.ExecCount.Load())
	fmt.Printf("成功提交的交易数目 %d \n", optmeInstance.Statistics.CommitCount.Load())

	tps := float64(optmeInstance.Statistics.CommitCount.Load()) / (elapsed.Seconds())
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

	defer optmeInstance.GetThreadPool().EvmClose()
	return [][]float64{[]float64{tps}, []float64{elapsed.Seconds()}}
}
