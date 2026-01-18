package janus

import (
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"fmt"
	"time"
)

// Run 运行 Janus 混合负载并发执行引擎
func Run(blockTxs []types.Transactions, levm *lvm.LEVM) float64 {
	fmt.Println("╔════════════════════════════════════════════════════╗")
	fmt.Println("║   Janus Hybrid Transaction Execution Engine        ║")
	fmt.Println("╚════════════════════════════════════════════════════╝")
	SetMWISSolver(SolverILP)
	SetMWISBenchmark(true)
	fmt.Printf("Thread Pool Size: %d\n", janusConfig.AllThreadNum)
	fmt.Printf("Water Mark Alpha: %.1f\n", janusConfig.WaterMarkAlpha)
	fmt.Printf("Water Mark Beta: %.1f\n\n", janusConfig.WaterMarkBeta)

	blockNum := janusConfig.TxNum / janusConfig.BlockSize
	numThreads := janusConfig.AllThreadNum

	// 创建批次生成器
	batchGenerator := NewBatchGenerator(numThreads)

	// 预先生成所有区块的批次
	batchForBlock := make([][]*Batch, blockNum)
	jtxss := make([][]*janusTransaction, blockNum)
	totalBatches := 0
	for i := 0; i < blockNum; i++ {
		txs := blockTxs[i]
		batches, jtxs := batchGenerator.GenerateBatches(txs)
		batchForBlock[i], jtxss[i] = batches, jtxs
		totalBatches += len(batches)

		fmt.Printf("Block %d: Generated %d batches (%d transactions)\n",
			i, len(batches), len(txs))
	}

	start := time.Now()

	// 创建流水线引擎
	pipeline := NewPipelineEngine(levm, numThreads)

	//tools.CatStorageState = true
	// 启动流水线（8个工作线程）
	pipeline.Start()

	// 统计结果
	allResults := make([]*ValidationResult, 0, totalBatches)

	// 按区块顺序处理
	for i := 0; i < blockNum; i++ {
		fmt.Printf("\n╔════════════════════════════════════════════════════╗\n")
		fmt.Printf("║            Processing Block %d                      ║\n", i)
		fmt.Printf("╚════════════════════════════════════════════════════╝\n")

		batches := batchForBlock[i]
		fmt.Printf("Block %d has %d batches:\n", i, len(batches))
		for _, batch := range batches {
			fmt.Printf("  Batch %d: %d long txs, %d short txs (total: %d, watermark: tx %d)\n",
				batch.ID, len(batch.LongTxs), len(batch.ShortTxs),
				len(batch.AllTxs), batch.WatermarkID)
		}

		// 提交当前区块的所有批次（一次性设置）
		pipeline.SubmitBlockBatches(batches, jtxss[i])

		// 等待当前区块的所有批次完成
		fmt.Printf("\n[Block %d] Waiting for %d batches to complete...\n", i, len(batches))
		blockResults := pipeline.WaitForBlockCompletion(len(batches))

		// 收集结果
		allResults = append(allResults, blockResults...)

		// 统计当前区块结果
		blockCommitted := 0
		blockAborted := 0
		for _, result := range blockResults {
			blockCommitted += len(result.CommittedTxs)
			blockAborted += len(result.AbortedTxs)
		}

		fmt.Printf("\n[Block %d Summary] Committed: %d, Aborted: %d\n",
			i, blockCommitted, blockAborted)
	}

	// 停止流水线
	fmt.Println("\n--- Stopping pipeline ---")
	pipeline.Stop()

	elapsed := time.Since(start)

	// 统计总体结果
	totalCommitted := 0
	totalAborted := 0
	for _, result := range allResults {
		totalCommitted += len(result.CommittedTxs)
		totalAborted += len(result.AbortedTxs)
	}

	fmt.Println("\n╔════════════════════════════════════════════════════╗")
	fmt.Println("║           Janus Execution Summary                 ║")
	fmt.Println("╠════════════════════════════════════════════════════╣")
	fmt.Printf("║ Thread Pool Size:         %-22d ║\n", numThreads)
	fmt.Printf("║ Total Execution Time:     %-22v ║\n", elapsed)
	fmt.Printf("║ Blocks Processed:         %-22d ║\n", blockNum)
	fmt.Printf("║ Batches Processed:        %-22d ║\n", totalBatches)
	fmt.Printf("║ Total Transactions:       %-22d ║\n", janusConfig.TxNum)
	fmt.Printf("║ Committed Transactions:   %-22d ║\n", totalCommitted)
	fmt.Printf("║ Aborted Transactions:     %-22d ║\n", totalAborted)

	txNumber := janusConfig.TxNum
	tps := float64(txNumber) / elapsed.Seconds()
	commitRate := float64(totalCommitted) / float64(txNumber) * 100

	fmt.Printf("║ Commit Rate:              %-21.2f%% ║\n", commitRate)
	fmt.Printf("║ TPS (Throughput):         %-22.2f ║\n", tps)
	fmt.Println("╚════════════════════════════════════════════════════╝")

	return tps
}
