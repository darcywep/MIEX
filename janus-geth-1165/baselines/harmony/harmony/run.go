package harmony

import (
	"fmt"
	"janus-geth-1165/baselines/common"
	janusConfig "janus-geth-1165/config"
	lvm "janus-geth-1165/core/evm"
	"janus-geth-1165/ethereum/core/types"
	"time"
)

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) float64 {
	txGenerator := common.NewTxGenerator(janusConfig.TxNum, janusConfig.BlockSize)
	blocks := txGenerator.GenerateWorkload(blockTxs) // 生成区块
	//fmt.Printf("Blocks num: %d, Blocks size: %d\n", len(blocks), len(blocks[0].Txs))

	static := common.NewStatistics()
	harmonyInstance := NewHarmony(blocks, static, janusConfig.AllThreadNum, 4, true)

	startTime := time.Now()
	harmonyInstance.Start(levm)
	elapsed := time.Since(startTime)

	fmt.Printf("CommitCount= %d \n", harmonyInstance.Statistics.CommitCount.Load())
	fmt.Printf("交易实际被执行总次数 %d \n", harmonyInstance.Statistics.ExecCount.Load())
	tps := float64(harmonyInstance.Statistics.CommitCount.Load()) / (elapsed.Seconds())
	fmt.Printf("交易处理吞吐(TPS)= %f \n", tps)
	return tps
}
