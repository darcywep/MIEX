package experiment

import (
	"bufio"
	"chukonu/config"
	"chukonu/core"
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/database"
	"chukonu/ethdb"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
)

const (
	threadNum  = 4
	testTxsLen = 10000
	compareLen = 500
	tpsTxs     = "../data/stm_tps.txt" // serial, block-stm
)

func TestBlockSTMTPSByLarge() {
	runtime.GOMAXPROCS(threadNum + 4)
	time.Sleep(100 * time.Millisecond)
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig)
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()
	var number uint64 = 14000000
	blockPre, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent     *types.Header  = blockPre.Header()
		parentRoot common.Hash    = parent.Root
		preRoot    common.Hash    = parent.Root
		stateCache state.Database = database.NewStateCache(db)

		txsLen                      = 0
		cknTxs                      = make(types.BlockStmTxs, 0)
		serialTime    time.Duration = 0
		serialTPS     float64       = 0
		allSerialTPS  float64       = 0
		allChuKoNuTPS float64       = 0
		cknTxIndex                  = 0
		count                       = 0
	)
	stateDB, _ := state.New(parentRoot, stateCache, nil)
	stmStateDB, _ := state.NewStmStateDB(parentRoot, stateCache, nil, stateDB) // 每个区块重新构建statedb以释放内存
	processor := core.NewStateProcessor(config.MainnetChainConfig, db)

	min, max, addSpan := big.NewInt(14000001), big.NewInt(14020001), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {

		block, err := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err != nil {
			fmt.Println(err)
			return
		}
		statedbCopy := stateDB.Copy()
		processor.Process(block, stateDB.Copy(), vm.Config{EnablePreimageRecording: false})

		start1 := time.Now()
		_, _, _, _, _, err = processor.Process(block, stateDB, vm.Config{EnablePreimageRecording: false})
		if err != nil {
			fmt.Println("process serial error", err)
			return
		}
		end1 := time.Since(start1)
		serialTime += end1

		for _, tx := range block.Transactions() {
			tx.Index = cknTxIndex
			cknTxs = append(cknTxs, types.NewBlockStmTx(tx, block, nil))
			cknTxIndex += 1
		}

		txsLen += block.Transactions().Len()
		if txsLen >= compareLen && serialTPS == 0 {
			serialTPS = float64(txsLen) / serialTime.Seconds()
			allSerialTPS += serialTPS
		}
		if txsLen >= testTxsLen { // 对比 testTxsLen 个交易
			cknTxs = cknTxs[:compareLen]
			stmStateDB, _ = state.NewStmStateDB(parentRoot, stateCache, nil, statedbCopy) // 每个区块重新构建statedb以释放内存

			start2 := time.Now()
			_, err = testBlockSTMByLarge(cknTxs, config.MainnetChainConfig, db, stmStateDB, threadNum)
			end2 := time.Since(start2)
			if err != nil {
				fmt.Println("process error", err)
				return
			}

			allChuKoNuTPS += float64(compareLen) / end2.Seconds()
			txsLen = 0
			cknTxs = cknTxs[:0]
			serialTime = 0
			serialTPS = 0
			cknTxIndex = 0
			count += 1
			if count == 10 {
				fmt.Println("Serial TPS:", allSerialTPS/10)
				fmt.Println("Block TPS:", allChuKoNuTPS/10)
				break
			}

			// Commit all cached state changes into underlying memory database.
			parentRoot, _ = stateDB.Commit(config.MainnetChainConfig.IsEIP158(block.Number()))
			stateDB, _ = state.New(parentRoot, stateCache, nil)
			stateDB.Database().TrieDB().Reference(parentRoot, common.Hash{}) // metadata reference to keep trie alive
			stateDB, _ = state.New(parentRoot, stateCache, nil)
			stateDB.Database().TrieDB().Dereference(preRoot)
			preRoot = parentRoot
		}

		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String())
	}
}

func TestBlockSTMTPSByBlock() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig)
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()
	var number uint64 = 14000000
	blockPre, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent     *types.Header  = blockPre.Header()
		parentRoot common.Hash    = parent.Root
		preRoot    common.Hash    = parent.Root
		stateCache state.Database = database.NewStateCache(db)
		data       [][]float64    = make([][]float64, 0)
	)
	processor := core.NewStateProcessor(config.MainnetChainConfig, db)

	min, max, addSpan := big.NewInt(14000001), big.NewInt(14020001), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {

		block, err := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err != nil {
			fmt.Println(err)
			return
		}

		stateDB, _ := state.New(parentRoot, stateCache, nil)
		stmStateDB, _ := state.NewStmStateDB(parentRoot, stateCache, nil, stateDB.Copy()) // 每个区块重新构建statedb以释放内存
		processor.Process(block, stateDB.Copy(), vm.Config{EnablePreimageRecording: false})

		start1 := time.Now()
		_, _, _, _, _, err = processor.Process(block, stateDB, vm.Config{EnablePreimageRecording: false})
		if err != nil {
			fmt.Println("process serial error", err)
			return
		}
		end1 := time.Since(start1)

		start2 := time.Now()
		_, err = testBlockSTMByBlock(block, config.MainnetChainConfig, db, stmStateDB, threadNum)
		end2 := time.Since(start2)

		if err != nil {
			fmt.Println("process error", err)
			return
		}
		var serialTPS, blockSTMTPS, compare float64 = 0, 0, 0

		if block.Transactions().Len() != 0 {
			txsLen := block.Transactions().Len()
			serialTPS = float64(txsLen) / end1.Seconds()
			blockSTMTPS = float64(txsLen) / end2.Seconds()
			compare = blockSTMTPS / serialTPS
			data = append(data, []float64{serialTPS, blockSTMTPS, compare})
		} else {
			data = append(data, []float64{0, 0, 0})
		}

		// Commit all cached state changes into underlying memory database.
		parentRoot, _ = stateDB.Commit(config.MainnetChainConfig.IsEIP158(block.Number()))
		stateDB.Database().TrieDB().Reference(parentRoot, common.Hash{}) // metadata reference to keep trie alive
		stateDB.Database().TrieDB().Dereference(preRoot)
		preRoot = parentRoot

		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String(), serialTPS, blockSTMTPS, compare)
	}
	// 打开或创建一个文本文件，如果文件已存在则会被覆盖
	file, err := os.Create(tpsTxs)
	if err != nil {
		fmt.Println("无法创建文件:", err)
		return
	}
	defer file.Close()

	// 创建一个写入器，用于将数据写入文件
	writer := bufio.NewWriter(file)

	// 将数据写入文件
	for _, row := range data {
		_, err := fmt.Fprintf(writer, "%.2f %.2f %.2f\n", row[0], row[1], row[2])
		if err != nil {
			fmt.Println("写入文件失败:", err)
			return
		}
	}

	// 刷新缓冲区以确保数据被写入文件
	err = writer.Flush()
	if err != nil {
		fmt.Println("刷新缓冲区失败:", err)
		return
	}
	fmt.Println("文件写入成功！")
}

func testBlockSTMByBlock(block *types.Block, config *params.ChainConfig, chainDb ethdb.Database, stateDB *state.StmStateDB, threads int) (*common.Hash, error) {
	var wg sync.WaitGroup
	stmProcessor := core.NewScheduler(block, nil, config, chainDb)
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go func() {
			defer wg.Done()
			newThread := core.NewThread(stmProcessor, stateDB, chainDb)
			newThread.Run()
		}()
	}
	wg.Wait()
	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !config.IsShanghai(block.Time()) {
		return nil, fmt.Errorf("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	stateDB.FinaliseMVMemory()
	header := stmProcessor.GetBlockInfo().Header
	core.StmAccumulateRewards(config, stateDB, header, block.Uncles())
	root := stateDB.IntermediateRoot(config.IsEIP158(header.Number))

	//txNum := len(block.Transactions())
	//avgExe := float64(stmProcessor.GetExecutionCounter()) / float64(txNum)
	//fmt.Printf("Number of txs: %d\n", txNum)
	//fmt.Printf("Re-execution overhead: %d, average overhead: %.2f\n", stmProcessor.GetExecutionCounter(), avgExe)

	return &root, nil
}

func testBlockSTMByLarge(txs types.BlockStmTxs, config *params.ChainConfig, chainDb ethdb.Database, stateDB *state.StmStateDB, threads int) (*common.Hash, error) {
	var wg sync.WaitGroup
	stmProcessor := core.NewScheduler(nil, txs, config, chainDb)
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go func() {
			defer wg.Done()
			newThread := core.NewThread(stmProcessor, stateDB, chainDb)
			newThread.Run()
		}()
	}
	wg.Wait()
	//// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	//withdrawals := block.Withdrawals()
	//if len(withdrawals) > 0 && !config.IsShanghai(block.Time()) {
	//	return nil, fmt.Errorf("withdrawals before shanghai")
	//}
	//// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	stateDB.FinaliseMVMemory()
	header := stmProcessor.GetBlockInfo().Header
	//core.StmAccumulateRewards(config, stateDB, header, block.Uncles())
	root := stateDB.IntermediateRoot(config.IsEIP158(header.Number))

	//txNum := len(block.Transactions())
	//avgExe := float64(stmProcessor.GetExecutionCounter()) / float64(txNum)
	//fmt.Printf("Number of txs: %d\n", txNum)
	//fmt.Printf("Re-execution overhead: %d, average overhead: %.2f\n", stmProcessor.GetExecutionCounter(), avgExe)

	return &root, nil
}
