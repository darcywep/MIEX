package schain

import (
	"fmt"
	janusConfig "janus-geth-1165/config"
	lvm "janus-geth-1165/core/evm"
	"janus-geth-1165/ethereum/config"
	"janus-geth-1165/ethereum/core/types"
	"janus-geth-1165/ethereum/database"
	"time"

	"github.com/ethereum/go-ethereum/params"
)

var stateConfig *database.StateDBConfig
var chainConfig *params.ChainConfig

func init() {
	stateConfig = &database.StateDBConfig{
		Path:    "/root/alldb/smallbank_database",
		Cache:   16000,
		Handles: 16000,
	}
	chainConfig = config.TestChainConfig
}

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) float64 {
	fmt.Println("=== Run SChain ===")

	blockNum := janusConfig.TxNum / janusConfig.BlockSize

	start2 := time.Now()
	for i := 0; i < blockNum; i++ {
		// Step 3: 模拟执行
		levmCopy := levm.Copy()
		//schain.TestSerialExecution(blockTxs[i], levm)

		GetRWSetByOCC(blockTxs[i], levmCopy)
		//fmt.Println("finished occ")
		//time.Sleep(1 * time.Second)

		//tools.CatStorageState = true
		//schain.SChain(blockTxs[i], levm)
		SChainParallelUp(blockTxs[i], levmCopy)

		//levmCopy.CommitStateChange()

	}
	end2 := time.Since(start2)
	txNumber := janusConfig.TxNum
	tps := float64(txNumber) / end2.Seconds()
	fmt.Println("SChian TPS:", tps)
	return tps
}
