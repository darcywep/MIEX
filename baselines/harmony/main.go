package main

func main() {
	//txGenerator := common.NewTxGenerator(config.AllBlocksTxSum, config.BlockSize)
	//blocks := txGenerator.GenerateWorkload() // 生成区块
	////fmt.Printf("Blocks num: %d, Blocks size: %d\n", len(blocks), len(blocks[0].Txs))
	//
	//static := common.NewStatistics()
	//harmonyInstance := harmony.NewHarmony(blocks, static, 4, 4, true)
	//
	//startTime := time.Now()
	//harmonyInstance.Start()
	//elapsed := time.Since(startTime)
	//
	//fmt.Printf("CommitCount= %d \n", harmonyInstance.Statistics.CommitCount.Load())
	//fmt.Printf("交易实际被执行总次数 %d \n", harmonyInstance.Statistics.ExecCount.Load())
	//fmt.Printf("交易处理吞吐(TPS)= %f \n", float64(harmonyInstance.Statistics.CommitCount.Load())/(elapsed.Seconds()))
}
