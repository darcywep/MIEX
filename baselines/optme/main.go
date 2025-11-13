package main

import (
	"Janus/baselines/common"
	"Janus/baselines/optme/optme"
	"Janus/config"
	"fmt"
	"time"
)

func main() {

	fmt.Println("optme start")

	txGenerator := common.NewTxGenerator(config.TxNum, config.BlockSize)

	blocks := txGenerator.GenerateWorkload() // 生成区块
	fmt.Printf("Blocks num: %d\n", len(blocks))
	fmt.Printf("Blocks size: %d\n", len(blocks[0].Txs))

	static := common.NewStatistics()
	optmeInstance := optme.NewOptME(blocks, static, 2, 4, true)

	startTime := time.Now()
	optmeInstance.Start()
	elapsed := time.Since(startTime)

	fmt.Printf("被执行的交易数目 %d \n", optmeInstance.Statistics.ExecCount.Load())
	fmt.Printf("成功提交的交易数目 %d \n", optmeInstance.Statistics.CommitCount.Load())
	fmt.Printf("交易处理吞吐(TPS)= %f \n", float64(optmeInstance.Statistics.CommitCount.Load())/(elapsed.Seconds()))

	defer optmeInstance.GetThreadPool().EvmClose()
}
