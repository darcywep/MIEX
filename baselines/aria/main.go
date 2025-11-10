package main

import (
	"Janus/baselines/aria/aria"
	janusCommon "Janus/plugin/Common"
	"Janus/plugin/Optme"
	"fmt"
	"time"
)

func main() {

	txGenerator := Optme.NewTxGenerator(janusCommon.TX_NUM, janusCommon.BLOCK_SIZE) // TX_NUM = 2000, BLOCK_SIZE = 1000

	blocks := txGenerator.GenerateWorkload() // 生成区块
	fmt.Printf("Blocks num: %d\n", len(blocks))
	fmt.Printf("Blocks size: %d\n", len(blocks[0].Txs))

	static := janusCommon.NewStatistics()
	aria := aria.NewAria(blocks, static, 2, 4, true)
	start := time.Now()
	//tools.CatStorageState = true
	aria.Start()
	aria.Stop()
	fmt.Println("CommitCount=", aria.Statistics().CommitCount.Load())
	fmt.Println("Aria TPS: ", float64(aria.Statistics().CommitCount.Load())/(time.Since(start).Seconds()))

	defer aria.EvmClose()
}
