package main

import (
	"Janus/baselines/common"
	"Janus/baselines/optme/optme"
	"Janus/config"
	"fmt"
)

func main() {

	fmt.Println("optme start")

	txGenerator := common.NewTxGenerator(config.TxNum, config.BlockSize) // TX_NUM = 2000, BLOCK_SIZE = 1000

	blocks := txGenerator.GenerateWorkload() // 生成区块
	fmt.Printf("Blocks num: %d\n", len(blocks))
	fmt.Printf("Blocks size: %d\n", len(blocks[0].Txs))

	static := common.NewStatistics()
	optme := optme.NewOptME(blocks, static, 2, 4, true)
	optme.Start()

	defer optme.GetThreadPool().EvmClose()
}
