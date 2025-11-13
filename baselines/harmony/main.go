package main

import (
	"Janus/baselines/common"
	"Janus/baselines/harmony/harmony"
	"Janus/config"
	"fmt"
	"time"
)

func main() {
	txGenerator := common.NewTxGenerator(config.TxNum, config.BlockSize) // TX_NUM = 2000, BLOCK_SIZE = 1000
	blocks := txGenerator.GenerateWorkload()                             // 生成区块
	fmt.Printf("Blocks num: %d, Blocks size: %d\n", len(blocks), len(blocks[0].Txs))

	static := common.NewStatistics()
	harmonyInstance := harmony.NewHarmony(blocks, static, 4, 4, true)
	harmonyInstance.Start()

	startTime := time.Now()
	elapsed := time.Since(startTime)

	fmt.Printf("被执行的交易数目 %d \n", harmonyInstance.Statistics.ExecCount.Load())
	fmt.Printf("成功提交的交易数目 %d \n", harmonyInstance.Statistics.CommitCount.Load())
	fmt.Printf("交易处理吞吐(TPS) %f \n", float64(harmonyInstance.Statistics.CommitCount.Load())/(elapsed.Seconds()))
}
