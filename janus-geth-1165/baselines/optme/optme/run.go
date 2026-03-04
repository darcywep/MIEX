package optme

import (
	"fmt"
	"janus-geth-1165/baselines/common"
	janusConfig "janus-geth-1165/config"
	lvm "janus-geth-1165/core/evm"
	"janus-geth-1165/ethereum/core/types"
	"time"
)

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) float64 {

	fmt.Println("optme start")

	txGenerator := common.NewTxGenerator(janusConfig.TxNum, janusConfig.BlockSize)

	blocks := txGenerator.GenerateWorkload(blockTxs) // 生成区块
	fmt.Printf("Blocks num: %d\n", len(blocks))
	fmt.Printf("Blocks size: %d\n", len(blocks[0].Txs))

	static := common.NewStatistics()
	optmeInstance := NewOptME(blocks, static, janusConfig.AllThreadNum, 4, true, levm)

	startTime := time.Now()
	optmeInstance.Start()
	elapsed := time.Since(startTime)

	fmt.Printf("被执行的交易数目 %d \n", optmeInstance.Statistics.ExecCount.Load())
	fmt.Printf("成功提交的交易数目 %d \n", optmeInstance.Statistics.CommitCount.Load())
	tps := float64(optmeInstance.Statistics.CommitCount.Load()) / (elapsed.Seconds())
	fmt.Printf("交易处理吞吐(TPS)= %f \n", tps)

	defer optmeInstance.GetThreadPool().EvmClose()
	return tps
}
