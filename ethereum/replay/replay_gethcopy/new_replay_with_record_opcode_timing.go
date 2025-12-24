package replay_gethcopy

import (
	"Janus/ethereum/config"
	"Janus/ethereum/core/vm"
	"Janus/ethereum/database"
	"Janus/ethereum/ethdb"
	"Janus/ethereum/replay/replay_config"
	"fmt"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

const (
	// 128GB内存配置
	TrieMemoryLimit  = 32 * 1024 * 1024 * 1024 // 64GB - trie节点
	ImageMemoryLimit = 5 * 1024 * 1024 * 1024  // 5GB - 图像

	// 激进的刷盘策略（这些是关键）
	CommitInterval   = 50  // 每50个区块完全Commit（真正释放内存）
	CapInterval      = 20  // 每20个区块Cap（整理内存）
	ForceGCInterval  = 100 // 每100个区块强制GC
	MemCheckInterval = 10  // 每10个区块检查内存
)

func Reference(alldb *database.AllDBForState, parentRoot, root common.Hash) {
	// 引用当前root
	alldb.TrieDB.Reference(root, common.Hash{})

	// 立即解除上一个root的引用
	alldb.TrieDB.Dereference(parentRoot)

	// 检查并清理内存
	_, nodes, imgs := alldb.TrieDB.Size()
	limit := common.StorageSize(TrieMemoryLimit)
	imgLimit := common.StorageSize(ImageMemoryLimit)

	if nodes > limit || imgs > imgLimit {
		fmt.Printf("[Memory Warning] Nodes: %v/%v, Images: %v/%v\n",
			common.StorageSize(nodes), limit,
			common.StorageSize(imgs), imgLimit)
		alldb.TrieDB.Cap(limit - ethdb.IdealBatchSize)
	}
}

func ReplayWithRecordOpCodeTiming() {
	processor, frdb, err := newProcessor()
	if err != nil {
		panic(err)
		return
	}
	defer frdb.Close()

	blockPre, err := database.GetBlockByNumber(frdb, replay_config.RootBlockNumber)
	if err != nil {
		panic(err)
		return
	}

	var parentStateRoot = blockPre.Root()
	alldbForState, err := database.NewAllDBForState(
		database.DefaultStateDBConfig,
		blockPre.Number(),
		blockPre.Root(),
		false,
		false,
	)
	if err != nil {
		panic(err)
	}
	defer alldbForState.Close()

	vm.InitInstructionTimer(vm.TimingDataFile)

	fmt.Printf("\n")
	fmt.Printf("╔═══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  Replay with Aggressive Memory Management (128GB Config)\n")
	fmt.Printf("╠═══════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Memory Limits:\n")
	fmt.Printf("║    • Trie:   %v\n", common.StorageSize(TrieMemoryLimit))
	fmt.Printf("║    • Images: %v\n", common.StorageSize(ImageMemoryLimit))
	fmt.Printf("║  Strategy:\n")
	fmt.Printf("║    • Commit every %d blocks (真正释放内存)\n", CommitInterval)
	fmt.Printf("║    • Cap every %d blocks (整理内存)\n", CapInterval)
	fmt.Printf("║    • GC every %d blocks (Go垃圾回收)\n", ForceGCInterval)
	fmt.Printf("╚═══════════════════════════════════════════════════════════╝\n\n")

	processStartTime := time.Now()
	blockCount := uint64(0)

	// 设置更激进的GC参数
	debug.SetGCPercent(50) // GC触发阈值降低到50%（默认100%）

	for blockNumber := replay_config.StartBlockNumber; blockNumber.Cmp(replay_config.FinishBlockNumber) == -1; blockNumber = blockNumber.Add(blockNumber, replay_config.AddSpan) {

		blockCount++

		// 更新StateDB
		err := alldbForState.UpdateStateDB(parentStateRoot)
		if err != nil {
			panic(err)
			return
		}

		block, err := database.GetBlockByNumber(frdb, blockNumber)
		if err != nil {
			panic(err)
			return
		}

		// Copy StateDB（必须的）
		statedbCopy := alldbForState.StateDB.Copy()

		// 第一次执行
		vm.TimingEnabled = false
		_, err = processor.Process(block, alldbForState.StateDB, config.DefaultVmConfig)
		if err != nil {
			fmt.Println("First process error:", err)
		}

		// 第二次执行
		vm.TimingEnabled = true
		_, err = processor.Process(block, statedbCopy, config.DefaultVmConfig)
		if err != nil {
			fmt.Println("Second process error:", err)
		}

		// 提交状态
		root, _, err := alldbForState.StateDB.CommitWithUpdate(
			block.NumberU64(),
			config.MainnetChainConfig.IsEIP158(block.Number()),
			config.MainnetChainConfig.IsCancun(block.Number(), block.Time()),
		)
		if err != nil {
			fmt.Println("Commit error:", err)
		}

		// 验证
		if root != block.Root() {
			fmt.Printf("⚠️  Root mismatch at block %v: expected %v, got %v\n",
				blockNumber, block.Root(), root)
		}
		fmt.Println("blockNumber="+blockNumber.String()+"\t process state root:", block.Root())
		fmt.Println("blockNumber="+blockNumber.String()+"\t block state root  :", root)

		// Copy用完立即置nil
		statedbCopy = nil

		// Reference/Dereference管理
		Reference(alldbForState, parentStateRoot, root)
		parentStateRoot = root

		// 每N个区块Cap内存
		if blockCount%CapInterval == 0 {
			_, nodes, imgs := alldbForState.TrieDB.Size()
			fmt.Printf("\n[Cap] Block %d - Before: Nodes=%v, Images=%v\n",
				blockNumber, common.StorageSize(nodes), common.StorageSize(imgs))

			alldbForState.TrieDB.Cap(common.StorageSize(TrieMemoryLimit) - ethdb.IdealBatchSize)

			_, nodesAfter, imgsAfter := alldbForState.TrieDB.Size()
			fmt.Printf("[Cap] After: Nodes=%v, Images=%v (Freed: %v)\n",
				common.StorageSize(nodesAfter), common.StorageSize(imgsAfter),
				common.StorageSize((nodes+imgs)-(nodesAfter+imgsAfter)))
		}

		// 每N个区块完全Commit（关键！这个才能真正释放内存）
		if blockCount%CommitInterval == 0 {
			_, nodes, imgs := alldbForState.TrieDB.Size()
			fmt.Printf("\n[Commit] Block %d - Before: Nodes=%v, Images=%v\n",
				blockNumber, common.StorageSize(nodes), common.StorageSize(imgs))
			fmt.Printf("[Commit] Performing full commit to disk...\n")

			commitStart := time.Now()
			// Commit(root, true) - true表示同时清理缓存
			err = alldbForState.TrieDB.Commit(root, true)
			if err != nil {
				fmt.Printf("⚠️  Commit error: %v\n", err)
			}
			commitDuration := time.Since(commitStart)

			_, nodesAfter, imgsAfter := alldbForState.TrieDB.Size()
			fmt.Printf("[Commit] After: Nodes=%v, Images=%v (Freed: %v)\n",
				common.StorageSize(nodesAfter), common.StorageSize(imgsAfter),
				common.StorageSize((nodes+imgs)-(nodesAfter+imgsAfter)))
			fmt.Printf("[Commit] Duration: %v\n", commitDuration)
		}

		// 每N个区块强制GC
		if blockCount%ForceGCInterval == 0 {
			var m1, m2 runtime.MemStats
			runtime.ReadMemStats(&m1)

			fmt.Printf("\n[Go GC] Block %d - Before: Alloc=%v, Sys=%v\n",
				blockNumber, common.StorageSize(m1.Alloc), common.StorageSize(m1.Sys))

			runtime.GC()
			debug.FreeOSMemory() // 强制归还内存给OS

			runtime.ReadMemStats(&m2)
			fmt.Printf("[Go GC] After: Alloc=%v, Sys=%v (Freed: %v)\n",
				common.StorageSize(m2.Alloc), common.StorageSize(m2.Sys),
				common.StorageSize(m1.Alloc-m2.Alloc))
		}

		// 定期检查内存
		if blockCount%MemCheckInterval == 0 {
			_, nodes, imgs := alldbForState.TrieDB.Size()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			if blockCount%100 == 0 {
				// 详细报告
				fmt.Printf("\n")
				fmt.Printf("═══════════════════════════════════════════\n")
				fmt.Printf(" Block %d Progress Report\n", blockNumber)
				fmt.Printf("═══════════════════════════════════════════\n")
				fmt.Printf("Trie DB:\n")
				fmt.Printf("  Nodes:  %v / %v (%.1f%%)\n",
					common.StorageSize(nodes), common.StorageSize(TrieMemoryLimit),
					float64(nodes)/float64(TrieMemoryLimit)*100)
				fmt.Printf("  Images: %v / %v (%.1f%%)\n",
					common.StorageSize(imgs), common.StorageSize(ImageMemoryLimit),
					float64(imgs)/float64(ImageMemoryLimit)*100)
				fmt.Printf("Go Runtime:\n")
				fmt.Printf("  Alloc:  %v\n", common.StorageSize(m.Alloc))
				fmt.Printf("  Sys:    %v\n", common.StorageSize(m.Sys))
				fmt.Printf("  NumGC:  %d\n", m.NumGC)
				fmt.Printf("Performance:\n")
				fmt.Printf("  Blocks: %d\n", blockCount)
				fmt.Printf("  Avg:    %v/block\n", time.Since(processStartTime)/time.Duration(blockCount))
				fmt.Printf("═══════════════════════════════════════════\n\n")
			} else {
				// 简单进度
				fmt.Printf("[Progress] Block %d | Trie: %v | Go: %v | Avg: %v\n",
					blockNumber, common.StorageSize(nodes+imgs), common.StorageSize(m.Alloc),
					time.Since(processStartTime)/time.Duration(blockCount))
			}
		}

		// 每1000个区块检查计时器
		if blockNumber.Uint64()%1000 == 0 {
			vm.GetInstructionTimer().CheckCompletionUnlocked()
		}
	}

	totalTime := time.Since(processStartTime)
	fmt.Printf("\n")
	fmt.Printf("╔═══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  ✓ Replay Completed\n")
	fmt.Printf("╠═══════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Blocks:    %d\n", blockCount)
	fmt.Printf("║  Duration:  %v\n", totalTime)
	fmt.Printf("║  Avg:       %v/block\n", totalTime/time.Duration(blockCount))
	fmt.Printf("╚═══════════════════════════════════════════════════════════╝\n")
}
