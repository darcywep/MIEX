package aria

import (
	"fmt"
	"Janus/baselines/common"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"time"
)

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) []float64 {
	start := time.Now()
	txGenerator := common.NewTxGenerator(janusConfig.AllBlocksTxSum, janusConfig.BlockSize) // TX_NUM = 2000, BLOCK_SIZE = 1000

	blocks := txGenerator.GenerateWorkload(blockTxs) // 生成区块
	//fmt.Printf("Blocks num: %d\n", len(blocks))
	//fmt.Printf("Blocks size: %d\n", len(blocks[0].Txs))

	static := common.NewStatistics()
	aria := NewAria(blocks, static, janusConfig.AllThreadNum, 4, true, levm)
	//tools.CatStorageState = true
	aria.Start()
	aria.Stop()
	latency := time.Since(start).Seconds()
	fmt.Println("CommitCount=", aria.Statistics().CommitCount.Load())
	tps := float64(aria.Statistics().CommitCount.Load()) / latency
	fmt.Println("Aria TPS: ", tps)

	defer aria.EvmClose()
	return []float64{tps, latency}
}
