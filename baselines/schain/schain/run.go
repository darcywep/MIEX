package schain

import (
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/config"
	"Janus/ethereum/core/types"
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

func Run() float64 {
	runtime.GOMAXPROCS(janusConfig.AllThreadNum + 2)
	fmt.Println("=== Run SChain ===")

	blockNum := janusConfig.TxNum / janusConfig.BlockSize
	blockTxs := make([]types.Transactions, 0) // 每个block的交易集合
	for i := 0; i < blockNum; i++ {
		txsLen := janusConfig.BlockSize
		// Step 1: 生成地址
		addresses := tools.GenerateAddresses(1, int(float64(txsLen)*janusConfig.AddressNumberRate))
		fmt.Printf("生成地址数量: %d\n", len(addresses))

		// Step 2: 生成交易（Zipf 控制冲突率）
		ethTxs := tools.GenerateSmallBankTxs(addresses, int(float64(txsLen)*janusConfig.CompetingTxCountRate), int(float64(txsLen)*janusConfig.IoTxCountRate),
			janusConfig.FibonacciN, janusConfig.RecursiveCalculateFibonacci, janusConfig.Skew)
		fmt.Printf("生成交易数量: %d\n", len(ethTxs)) // 生成以太坊交易
		blockTxs = append(blockTxs, ethTxs)
	}

	start2 := time.Now()
	levm := lvm.New(stateConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())
	defer levm.AllDB().Close()
	for i := 0; i < blockNum; i++ {
		// Step 3: 模拟执行

		//schain.TestSerialExecution(blockTxs[i], levm)

		GetRWSetByOCC(blockTxs[i], levm)
		//fmt.Println("finished occ")
		//time.Sleep(1 * time.Second)

		//tools.CatStorageState = true
		//schain.SChain(blockTxs[i], levm)
		SChainParallelUp(blockTxs[i], levm)

		root, err := levm.AllDB().StateDB.Commit(uint64(0), true, true)
		if err != nil {
			fmt.Println("StateDB.Commit", err)
		}
		err = levm.AllDB().StateDB.Database().TrieDB().Commit(root, false)
		if err != nil {
			fmt.Println("TrieDB().Commit(root, false)", err)
		}

	}
	end2 := time.Since(start2)
	txNumber := janusConfig.TxNum
	tps := float64(txNumber) / end2.Seconds()
	fmt.Println("SChian TPS:", tps)
	return tps
}
