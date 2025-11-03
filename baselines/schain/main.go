package main

import (
	"Janus/baselines/schain/schain"
	janusConfig "Janus/config"
	"Janus/ethereum/config"
	"Janus/ethereum/database"
	"Janus/tools"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/ethereum/go-ethereum/params"
)

const (
	txNumber = 10000
	skew     = 1.05
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
	addresses := tools.GenerateAddresses(0, janusConfig.AddressNumber)
	fmt.Printf("生成地址数量: %d\n", len(addresses))

	// Step 2: 生成交易（Zipf 控制冲突率）
	txs := tools.GenerateSmallBankTxs(addresses, janusConfig.IoTxCountForBlock,
		janusConfig.CompetingTxCountForBlock, janusConfig.FibonacciN, skew)
	fmt.Printf("生成交易数量: %d\n", len(txs))

	// Step 3: 模拟执行
	allDBForState, err := database.NewAllDBForState(stateConfig, big.NewInt(0), tools.StateRoot, false, false)
	tools.PanicError(err)
	defer allDBForState.Close()
	start2 := time.Now()
	schain.GetRWSetByOCC(txs, allDBForState.StateDB.Copy())

	schain.SChain(txs, allDBForState.StateDB)

	root, err := allDBForState.StateDB.Commit(uint64(0), true, true)
	if err != nil {
		fmt.Println("StateDB.Commit", err)
	}
	err = allDBForState.StateDB.Database().TrieDB().Commit(root, false)
	if err != nil {
		fmt.Println("TrieDB().Commit(root, false)", err)
	}
	end2 := time.Since(start2)
	if err != nil {
		fmt.Println("process error", err)
		return
	}
	fmt.Println(end2)
}
