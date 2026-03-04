package aria

import (
	"fmt"
	"janus-geth-1165/baselines/common"
	janusConfig "janus-geth-1165/config"
	lvm "janus-geth-1165/core/evm"
	"janus-geth-1165/ethereum/core/types"
	"time"
)

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) float64 {
	txGenerator := common.NewTxGenerator(janusConfig.TxNum, janusConfig.BlockSize) // TX_NUM = 2000, BLOCK_SIZE = 1000

	blocks := txGenerator.GenerateWorkload(blockTxs) // 生成区块
	//fmt.Printf("Blocks num: %d\n", len(blocks))
	//fmt.Printf("Blocks size: %d\n", len(blocks[0].Txs))

	static := common.NewStatistics()
	aria := NewAria(blocks, static, janusConfig.AllThreadNum, 4, true, levm)
	start := time.Now()
	//tools.CatStorageState = true
	aria.Start()
	aria.Stop()
	fmt.Println("CommitCount=", aria.Statistics().CommitCount.Load())
	tps := float64(aria.Statistics().CommitCount.Load()) / (time.Since(start).Seconds())
	fmt.Println("Aria TPS: ", tps)

	defer aria.EvmClose()
	return tps
}
