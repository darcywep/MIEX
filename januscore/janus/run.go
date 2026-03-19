package janus

import (
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"fmt"
	"time"
)

// Run 运行 Janus 混合负载并发执行引擎
func Run(blockTxs []types.Transactions, levm *lvm.LEVM) [][]float64 {
	fmt.Println("╔════════════════════════════════════════════════════╗")
	fmt.Println("║   Janus Hybrid Transaction Execution Engine        ║")
	fmt.Println("╚════════════════════════════════════════════════════╝")
	SetMWISSolver(SolverGreedy)
	SetMWISBenchmark(false)
	enableLog = false
	enableReExecutePhase1 = true
	enableReExecutePhase2 = true
	fmt.Printf("Thread Pool Size: %d\n", janusConfig.AllThreadNum)
	fmt.Printf("Water Mark Alpha: %.1f\n", janusConfig.WaterMarkAlpha)
	fmt.Printf("Water Mark Beta: %.1f\n\n", janusConfig.WaterMarkBeta)

	blockNum := janusConfig.AllBlocksTxSum / janusConfig.BlockSize
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

	abortTxs := make([]*janusTransaction, 0)

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

		abortTxs = append(abortTxs, pipeline.abortTxs...)
		AllJanusTransactions = append(AllJanusTransactions, jtxss[i])
	}

	// 停止流水线
	pipeline.Stop()

	elapsed := time.Since(start)
	var txsCosts float64 = 0
	for _, abortTx := range abortTxs {
		txsCosts += abortTx.ExecutionCost
	}
	tps := float64(janusConfig.AllBlocksTxSum) / elapsed.Seconds()
	//commitRate := float64(totalCommitted) / float64(janusConfig.AllBlocksTxSum) * 100
	commitRate := float64(committedTxsNum.Load()) / float64(janusConfig.AllBlocksTxSum) * 100

	fmt.Println("\n╔════════════════════════════════════════════════════╗")
	fmt.Println("║           Janus Execution Summary                 ║")
	fmt.Println("╠════════════════════════════════════════════════════╣")
	fmt.Printf("║ Thread Pool Size:         %-22d ║\n", numThreads)
	fmt.Printf("║ Total Execution Time:     %-22v ║\n", elapsed)
	fmt.Printf("║ Execute Current Batch Time:     %-22v ║\n", pipeline.timeOfExecuteCurrentBatchPhase)
	fmt.Printf("║ Execute Current Batch Tail Time:     %-22v ║\n", pipeline.timeOfExecuteCurrentBatchTail)
	fmt.Printf("║ Merge State Table Time:     %-22v ║\n", pipeline.timeOfMergeStateTablePhase)
	fmt.Printf("║ Construct DAG Time:     %-22v ║\n", pipeline.timeOfConstructDAGPhase)
	fmt.Printf("║ Construct Time!!!:     %-22v ║\n", pipeline.timeOfMergeStateTablePhase+pipeline.timeOfConstructDAGPhase-pipeline.timeOfExecuteCurrentBatchTail)
	fmt.Printf("║ Commit Maximum Validation Time:     %-22v ║\n", pipeline.timeOfCommitMaximumValidationPhase)
	fmt.Printf("║ Re-Execute Time:     %-22v ║\n", pipeline.timeOfReExecutePhase)
	fmt.Printf("║ Blocks Processed:         %-22d ║\n", blockNum)
	fmt.Printf("║ Batches Processed:        %-22d ║\n", totalBatches)
	fmt.Printf("║ Total Transactions:       %-22d ║\n", janusConfig.AllBlocksTxSum)
	//fmt.Printf("║ Committed Transactions:   %-22d ║\n", totalCommitted)
	fmt.Printf("║ Committed Transactions:   %-22d ║\n", committedTxsNum.Load())
	fmt.Printf("║ Aborted Transactions:     %-22d ║\n", totalCommitted-int(committedTxsNum.Load()))
	fmt.Printf("║ Commit Rate:              %-21.2f%% ║\n", commitRate)
	fmt.Printf("║ TPS (Throughput):         %-22.2f ║\n", tps)
	fmt.Printf("║ Number of abort transaction:         %-22d ║\n", len(abortTxs))
	fmt.Printf("║ Cost of abort transactions:         %-22.2f ║\n", txsCosts)
	fmt.Println("╚════════════════════════════════════════════════════╝")

	return [][]float64{[]float64{tps}, []float64{elapsed.Seconds()}}
}
