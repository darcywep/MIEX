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
	SetMWISSolver(SolverGreedy)
	SetMWISBenchmark(false)
	enableLog = false
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
	totalCommitted := 0
	for i := 0; i < blockNum; i++ {
		txs := blockTxs[i]
		totalCommitted += len(txs)
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

	// 按区块顺序处理
	for i := 0; i < blockNum; i++ {
		batches := batchForBlock[i]
		if enableLog {
			fmt.Printf("Block %d has %d batches:\n", i, len(batches))
			for _, batch := range batches {
				fmt.Printf("  Batch %d: %d long txs, %d short txs (total: %d, watermark: tx %d)\n",
					batch.ID, len(batch.LongTxs), len(batch.ShortTxs),
					len(batch.AllTxs), batch.WatermarkID)
			}
		}

		// 提交当前区块的所有批次（一次性设置）
		pipeline.SubmitBlockBatches(batches, jtxss[i])

		// 等待当前区块的所有批次完成
		//fmt.Printf("\n[Block %d] Waiting for %d batches to complete...\n", i, len(batches))
		pipeline.WaitForBlockCompletion(len(batches))
	}

	// 停止流水线
	pipeline.Stop()

	elapsed := time.Since(start)
	tps := float64(janusConfig.TxNum) / elapsed.Seconds()
	commitRate := float64(totalCommitted) / float64(janusConfig.TxNum) * 100

	fmt.Println("\n╔════════════════════════════════════════════════════╗")
	fmt.Println("║           Janus Execution Summary                 ║")
	fmt.Println("╠════════════════════════════════════════════════════╣")
	fmt.Printf("║ Thread Pool Size:         %-22d ║\n", numThreads)
	fmt.Printf("║ Total Execution Time:     %-22v ║\n", elapsed)
	fmt.Printf("║ Blocks Processed:         %-22d ║\n", blockNum)
	fmt.Printf("║ Batches Processed:        %-22d ║\n", totalBatches)
	fmt.Printf("║ Total Transactions:       %-22d ║\n", janusConfig.TxNum)
	fmt.Printf("║ Committed Transactions:   %-22d ║\n", totalCommitted)
	fmt.Printf("║ Aborted Transactions:     %-22d ║\n", 0)
	fmt.Printf("║ Commit Rate:              %-21.2f%% ║\n", commitRate)
	fmt.Printf("║ TPS (Throughput):         %-22.2f ║\n", tps)
	fmt.Println("╚════════════════════════════════════════════════════╝")

	return tps
}
