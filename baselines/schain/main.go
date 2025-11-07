package main

import (
	"Janus/baselines/schain/schain"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/config"
	"Janus/ethereum/database"
	"Janus/tools"
	"fmt"
	"runtime"
	"time"

	"math/big"

	"github.com/ethereum/go-ethereum/params"
)

var stateConfig *database.StateDBConfig
var chainConfig *params.ChainConfig

func init() {
	stateConfig = &database.StateDBConfig{
		Path:    "/root/alldb/smallbank_database",
		Cache:   4096,
		Handles: 4096,
	}
	chainConfig = config.TestChainConfig
}

func main() {
	runtime.GOMAXPROCS(janusConfig.AllThreadNum + 2)
	fmt.Println("=== Run SChain ===")

	// Step 1: 生成地址
	addresses := tools.GenerateAddresses(1, janusConfig.AddressNumber)
	fmt.Printf("生成地址数量: %d\n", len(addresses))

	// Step 2: 生成交易（Zipf 控制冲突率）
	txs := tools.GenerateSmallBankTxs(addresses, janusConfig.IoTxCountForBlock, janusConfig.CompetingTxCountForBlock,
		janusConfig.FibonacciN, janusConfig.RecursiveCalculateFibonacci, janusConfig.Skew)
	fmt.Printf("生成交易数量: %d\n", len(txs))

	// Step 3: 模拟执行
	levm := lvm.New(stateConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())
	defer levm.AllDB().Close()
	//schain.TestSerialExecution(txs, levm)
	start2 := time.Now()
	schain.GetRWSetByOCC(txs, levm)
	//fmt.Println("finished occ")
	//time.Sleep(1 * time.Second)

	//schain.SChain(txs, levm)
	schain.SChainParallelUp(txs, levm)

	root, err := levm.AllDB().StateDB.Commit(uint64(0), true, true)
	if err != nil {
		fmt.Println("StateDB.Commit", err)
	}
	err = levm.AllDB().StateDB.Database().TrieDB().Commit(root, false)
	if err != nil {
		fmt.Println("TrieDB().Commit(root, false)", err)
	}
	end2 := time.Since(start2)
	if err != nil {
		fmt.Println("process error", err)
		return
	}
	fmt.Println("TPS:", (janusConfig.CompetingTxCountForBlock+janusConfig.IoTxCountForBlock)/end2.Seconds())
}
