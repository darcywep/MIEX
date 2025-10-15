package main

import (
	"Janus/config"
	"Janus/file"
	"Janus/mempool"
	"Janus/monitor"
	"Janus/persister"
	"Janus/scheduler"
	"flag"
	"fmt"
	"runtime"
	"sync"
	"time"
)

func runAll(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
	fmt.Println("Running all...")
	time.Sleep(1 * time.Second)
	for i := 0; i < config.BlockSum; i++ { // 区块
		occTxSlice := make([]chan *config.Transaction, 0)
		for j := 0; j < (config.TxSum/2)/config.AllThreadNum; j++ { // 每个区块中的交易个数
			txChan := make(chan *config.Transaction, config.ChanLen)
			for i := 0; i < config.AllThreadNum; i++ {
				txChan <- mp.GetIOTx()
				txChan <- mp.GetCompetingTx()
			}
			occTxSlice = append(occTxSlice, txChan)
			//fmt.Println("occTxSlice: ", occTxSlice)
		}

		start := time.Now()
		for _, txChan := range occTxSlice { // 等待同一批次运行完成
			wg := new(sync.WaitGroup)
			close(txChan)
			wg.Add(config.AllThreadNum)
			for k := 0; k < config.AllThreadNum; k++ {
				go s.Run(stateCache, txChan, wg, k)
			}
			wg.Wait()
		}
		stateCache.Commit()
		fmt.Printf("Finished %d block, TPS: %f\n", i, config.TxSum/time.Since(start).Seconds())
	}
}

func runSep(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
	fmt.Println("Running sep...")
	time.Sleep(1 * time.Second)
	for i := 0; i < config.BlockSum; i++ { // 区块
		occIOTxSlice := make([]chan *config.Transaction, 0)
		occComputingTxSlice := make([]chan *config.Transaction, 0)
		for j := 0; j < (config.TxSum/2)/config.AllThreadNum; j++ { // 每个区块中的交易个数
			computingTxChan := make(chan *config.Transaction, config.ChanLen)
			ioTxChan := make(chan *config.Transaction, config.ChanLen)
			for j := 0; j < config.AllThreadNum/2; j++ { // 每个区块中的交易个数
				computingTxChan <- mp.GetCompetingTx()
				ioTxChan <- mp.GetIOTx()
			}
			occIOTxSlice = append(occIOTxSlice, ioTxChan)
			occComputingTxSlice = append(occComputingTxSlice, computingTxChan)
		}

		start := time.Now()
		for i, ioTxChan := range occIOTxSlice { // 等待同一批次运行完成
			computingTxChan := occComputingTxSlice[i]
			close(ioTxChan)
			close(computingTxChan)

			wg := new(sync.WaitGroup)
			wg.Add(config.AllThreadNum)
			for k := 0; k < config.ComputingThreadNum; k++ {
				go s.Run(stateCache, computingTxChan, wg, k)
			}
			for k := 0; k < config.IoThreadNum; k++ {
				go s.Run(stateCache, ioTxChan, wg, k)
			}
			wg.Wait()
		}
		stateCache.Commit()
		fmt.Printf("Finished %d block, TPS: %f\n", i, config.TxSum/time.Since(start).Seconds())
	}
}

func runComputing(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
	fmt.Println("Running computing threads")
	time.Sleep(1 * time.Second)
	for i := 0; i < config.BlockSum; i++ { // 区块
		occTxSlice := make([]chan *config.Transaction, 0)
		for j := 0; j < config.TxSum/config.AllThreadNum; j++ { // 每个区块中的交易个数
			txChan := make(chan *config.Transaction, config.ChanLen)
			for i := 0; i < config.AllThreadNum; i++ {
				txChan <- mp.GetCompetingTx()
			}
			occTxSlice = append(occTxSlice, txChan)
		}

		start := time.Now()
		for _, txChan := range occTxSlice { // 等待同一批次运行完成
			wg := new(sync.WaitGroup)
			close(txChan)
			wg.Add(config.AllThreadNum)
			for k := 0; k < config.AllThreadNum; k++ {
				go s.Run(stateCache, txChan, wg, k)
			}
			wg.Wait()
		}
		stateCache.Commit()
		fmt.Printf("Finished %d block, TPS: %f\n", i, config.TxSum/time.Since(start).Seconds())
	}
}

func runIO(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
	fmt.Println("Running IO threads...")
	time.Sleep(1 * time.Second)
	for i := 0; i < config.BlockSum; i++ { // 区块
		occTxSlice := make([]chan *config.Transaction, 0)
		for j := 0; j < config.TxSum/config.AllThreadNum; j++ { // 每个区块中的交易个数
			txChan := make(chan *config.Transaction, config.ChanLen)
			for i := 0; i < config.AllThreadNum; i++ {
				txChan <- mp.GetIOTx()
			}
			occTxSlice = append(occTxSlice, txChan)
		}

		start := time.Now()
		for _, txChan := range occTxSlice { // 等待同一批次运行完成
			wg := new(sync.WaitGroup)
			close(txChan)
			wg.Add(config.AllThreadNum)
			for k := 0; k < config.AllThreadNum; k++ {
				go s.Run(stateCache, txChan, wg, k)
			}
			wg.Wait()
		}
		stateCache.Commit()
		fmt.Printf("Finished %d block, TPS: %f\n", i, config.TxSum/time.Since(start).Seconds())
	}
}

func main() {
	mode := flag.String("m", "h",
		"mode: \n"+
			"\th stands for hybrid\n"+
			"\ts stands for Sep\n"+
			"\ti stands for io\n"+
			"\tc stands for compute\n")
	flag.Parse()

	fmt.Println("mode: ", *mode)

	runtime.GOMAXPROCS(config.AllThreadNum + 1)
	file.WriteFiles(config.FilePath, config.AllThreadNum)

	mp := mempool.NewMempool()
	if mp.ComputeTxs == nil {
		return
	}

	stateCache := persister.NewStateCache(config.JanusDBPath, config.Key2addrDBPath)
	// 调度执行
	s := scheduler.NewScheduler(stateCache)
	signalChan := make(chan struct{})
	signalWg := new(sync.WaitGroup)
	signalWg.Add(1)
	if *mode == "h" {
		go monitor.MonitorMetrics(1*time.Second, config.MonitorFilenameHybrid, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		runAll(stateCache, mp, s)
	} else if *mode == "s" {
		go monitor.MonitorMetrics(1*time.Second, config.MonitorFilenameSep, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		runSep(stateCache, mp, s)
	} else if *mode == "i" {
		go monitor.MonitorMetrics(1*time.Second, config.MonitorFilenameIO, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		runIO(stateCache, mp, s)
	} else if *mode == "c" {
		go monitor.MonitorMetrics(1*time.Second, config.MonitorFilenameCompute, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		runComputing(stateCache, mp, s)
	} else {
		fmt.Println("mode is invalid")
	}
	close(signalChan)
	signalWg.Wait()
}
