package main

import (
	"Janus/config"
	"Janus/mempool"
	"Janus/persister"
	"Janus/scheduler"
	"fmt"
	"runtime"
	"sync"
	"time"
)

const (
	allThreadNum       = 8
	ioThreadNum        = 2
	computingThreadNum = 6
	blockSum           = 1000  // 执行多少个区块
	chanLen            = 20000 // 每个区块有多少笔交易
	txSum              = 20000 // 每个区块有多少笔交易
	JanusDBPath        = "./JanusDB"
	key2addrDBPath     = "./key2addrDB"
)

func runAll(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
	fmt.Println("Running all...")
	wg := new(sync.WaitGroup)
	runtime.GOMAXPROCS(allThreadNum)
	for i := 0; i < blockSum; i++ { // 区块
		txChan := make(chan *config.Transaction, chanLen)

		for j := 0; j < txSum/2; j++ { // 每个区块中的交易个数
			txChan <- mp.GetIOTx()
			txChan <- mp.GetCompetingTx()
		}
		close(txChan)
		start := time.Now()
		wg.Add(allThreadNum)
		for k := 0; k < allThreadNum; k++ {
			go s.Run(stateCache, txChan, wg)
		}
		wg.Wait()
		stateCache.Commit()
		fmt.Printf("Finished %d block, TPS: %f\n", i, txSum/time.Since(start).Seconds())
		//time.Sleep(1 * time.Second)
	}
}

func runSep(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
	comWg := new(sync.WaitGroup)
	ioWg := new(sync.WaitGroup)
	runtime.GOMAXPROCS(allThreadNum)
	for i := 0; i < blockSum; i++ { // 区块
		computingTxChan := make(chan *config.Transaction, chanLen)
		ioTxChan := make(chan *config.Transaction, chanLen)
		for j := 0; j < txSum/2; j++ { // 每个区块中的交易个数
			computingTxChan <- mp.GetCompetingTx()
			ioTxChan <- mp.GetIOTx()
		}
		close(ioTxChan)
		close(computingTxChan)

		start := time.Now()
		comWg.Add(computingThreadNum)
		for k := 0; k < computingThreadNum; k++ {
			go s.Run(stateCache, computingTxChan, comWg)
		}
		comWg.Wait()

		ioWg.Add(ioThreadNum)
		for k := 0; k < ioThreadNum; k++ {
			go s.Run(stateCache, ioTxChan, ioWg)
		}
		ioWg.Wait()
		stateCache.Commit()
		fmt.Printf("Finished %d block, TPS: %f\n", i, txSum/time.Since(start).Seconds())
		//time.Sleep(5 * time.Second)
	}
}

func runComputing(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
	fmt.Println("Running computing threads")
	time.Sleep(1 * time.Second)
	comWg := new(sync.WaitGroup)
	runtime.GOMAXPROCS(allThreadNum)
	for i := 0; i < blockSum; i++ { // 区块
		computingTxChan := make(chan *config.Transaction, chanLen)
		for j := 0; j < txSum; j++ { // 每个区块中的交易个数
			computingTxChan <- mp.GetCompetingTx()
		}
		close(computingTxChan)

		start := time.Now()
		comWg.Add(allThreadNum)
		for k := 0; k < allThreadNum; k++ {
			go s.Run(stateCache, computingTxChan, comWg)
		}
		comWg.Wait()
		stateCache.Commit()
		fmt.Printf("Finished %d block, TPS: %f\n", i, txSum/time.Since(start).Seconds())
		//time.Sleep(1 * time.Second)
	}
}

func runIO(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
	fmt.Println("Running IO threads...")
	ioWg := new(sync.WaitGroup)
	runtime.GOMAXPROCS(allThreadNum)
	for i := 0; i < blockSum; i++ { // 区块
		ioTxChan := make(chan *config.Transaction, chanLen)
		for j := 0; j < txSum; j++ { // 每个区块中的交易个数
			ioTxChan <- mp.GetIOTx()
		}
		close(ioTxChan)

		start := time.Now()
		ioWg.Add(allThreadNum)
		for k := 0; k < allThreadNum; k++ {
			go s.Run(stateCache, ioTxChan, ioWg)
		}
		ioWg.Wait()
		stateCache.Commit()
		fmt.Printf("Finished %d block, time cost %f, TPS: %f\n", i, time.Since(start).Seconds(), txSum/time.Since(start).Seconds())
		//time.Sleep(1 * time.Second)
	}
}

func main() {
	mp := mempool.NewMempool()
	if mp.ComputeTxs == nil {
		return
	}

	stateCache := persister.NewStateCache(JanusDBPath, key2addrDBPath)
	// 调度执行
	s := scheduler.NewScheduler(stateCache)
	runAll(stateCache, mp, s)
	//runSep(stateCache, mp, s)
	//runSep(stateCache, mp, s)
	//runIO(stateCache, mp, s)
	//runComputing(stateCache, mp, s)
}
