package harmony

import (
	"Janus/baselines/common"
	janusConfig "Janus/config"
	"fmt"
	"time"
)

func Run() float64 {
	txGenerator := common.NewTxGenerator(janusConfig.TxNum, janusConfig.BlockSize)
	blocks := txGenerator.GenerateWorkload() // 生成区块
	fmt.Printf("Blocks num: %d, Blocks size: %d\n", len(blocks), len(blocks[0].Txs))

	static := common.NewStatistics()
	harmonyInstance := NewHarmony(blocks, static, janusConfig.AllThreadNum, 4, true)

	startTime := time.Now()
	harmonyInstance.Start()
	elapsed := time.Since(startTime)

	fmt.Printf("CommitCount= %d \n", harmonyInstance.Statistics.CommitCount.Load())
	fmt.Printf("交易实际被执行总次数 %d \n", harmonyInstance.Statistics.ExecCount.Load())
	tps := float64(harmonyInstance.Statistics.CommitCount.Load()) / (elapsed.Seconds())
	fmt.Printf("交易处理吞吐(TPS)= %f \n", tps)
	return tps
}
