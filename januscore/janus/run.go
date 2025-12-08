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
	fmt.Println("║   Janus Hybrid Transaction Execution Engine       ║")
	fmt.Println("╚════════════════════════════════════════════════════╝")
	fmt.Printf("Thread Pool Size: %d\n", janusConfig.AllThreadNum)
	fmt.Printf("Water Mark Alpha: %.1f\n", janusConfig.WaterMarkAlpha)
	fmt.Printf("Water Mark Beta: %.1f\n\n", janusConfig.WaterMarkBeta)

	blockNum := janusConfig.TxNum / janusConfig.BlockSize
	numThreads := janusConfig.AllThreadNum

	// 创建批次生成器
	batchGenerator := NewBatchGenerator(numThreads)
	batchForBlock := make([][]*Batch, blockNum)
	for i := 0; i < blockNum; i++ {
		txs := blockTxs[i]
		batches := batchGenerator.GenerateBatches(txs)
		batchForBlock[i] = batches
	}

	start := time.Now()

	// 创建流水线引擎
	pipeline := NewPipelineEngine(levm, numThreads)

	// 启动流水线（8个工作线程）
	pipeline.Start()

	totalBatches := 0

	// 处理每个区块
	for i := 0; i < blockNum; i++ {
		fmt.Printf("\n--- Processing Block %d (%d transactions) ---\n", i, len(blockTxs[i]))

		// 生成批次
		batches := batchForBlock[i]

		fmt.Printf("Generated %d batches:\n", len(batches))
		for j, batch := range batches {
			fmt.Printf("  Batch %d: %d long txs, %d short txs (total: %d)\n",
				j, len(batch.LongTxs), len(batch.ShortTxs), len(batch.AllTxs))
		}

		// 提交批次到流水线
		for j, batch := range batches {
			var nextBatch *Batch
			if j+1 < len(batches) {
				nextBatch = batches[j+1]
			}

			pipeline.SubmitBatch(batch, nextBatch)
			totalBatches++
		}
	}

	// 等待所有批次完成
	fmt.Println("\n--- Waiting for pipeline completion ---")
	results := pipeline.WaitForCompletion(totalBatches)

	// 停止流水线
	pipeline.Stop()

	elapsed := time.Since(start)

	// 统计结果
	totalCommitted := 0
	totalAborted := 0
	for _, result := range results {
		totalCommitted += len(result.CommittedTxs)
		totalAborted += len(result.AbortedTxs)
	}

	fmt.Println("\n╔════════════════════════════════════════════════════╗")
	fmt.Println("║           Janus Execution Summary                ║")
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
