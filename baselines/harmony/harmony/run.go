package harmony

import (
	"Janus/baselines/common"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"fmt"
	"time"
)

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) [][]float64 {
	txGenerator := common.NewTxGenerator(janusConfig.AllBlocksTxSum, janusConfig.BlockSize)
	blocks := txGenerator.GenerateWorkload(blockTxs) // 生成区块
	//fmt.Printf("Blocks num: %d, Blocks size: %d\n", len(blocks), len(blocks[0].Txs))

	startTime := time.Now()
	static := common.NewStatistics()
	harmonyInstance := NewHarmony(blocks, static, janusConfig.AllThreadNum, 4, true)
	harmonyInstance.Start(levm)
	elapsed := time.Since(startTime)

	fmt.Printf("CommitCount= %d \n", harmonyInstance.Statistics.CommitCount.Load())
	fmt.Printf("交易实际被执行总次数 %d \n", harmonyInstance.Statistics.ExecCount.Load())
	tps := float64(harmonyInstance.Statistics.CommitCount.Load()) / (elapsed.Seconds())
	fmt.Printf("交易处理吞吐(TPS)= %f \n", tps)
	return [][]float64{[]float64{tps}, []float64{elapsed.Seconds()}}
}
