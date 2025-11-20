package main

func main() {
	//
	//txGenerator := common.NewTxGenerator(config.TxNum, config.BlockSize)
	//
	//blocks := txGenerator.GenerateWorkload() // 生成区块
	//fmt.Printf("Blocks num: %d\n", len(blocks))
	//fmt.Printf("Blocks size: %d\n", len(blocks[0].Txs))
	//
	//static := common.NewStatistics()
	//aria := aria.NewAria(blocks, static, 4, 4, true)
	//start := time.Now()
	////tools.CatStorageState = true
	//aria.Start()
	//aria.Stop()
	//fmt.Println("CommitCount=", aria.Statistics().CommitCount.Load())
	//fmt.Println("Aria TPS: ", float64(aria.Statistics().CommitCount.Load())/(time.Since(start).Seconds()))
	//fmt.Printf("交易实际被执行总次数 %d \n", aria.Statistics().ExecCount.Load())
	//
	//defer aria.EvmClose()
}
