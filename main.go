package main

import (
	"MixedLoadTransactionConcurrency/config"
	"MixedLoadTransactionConcurrency/mempool"
	"MixedLoadTransactionConcurrency/persister"
	"MixedLoadTransactionConcurrency/scheduler"
	"fmt"
	"runtime"
	"sync"
)

func runAll() {
	wg := new(sync.WaitGroup)

}

func main() {
	// 初始化 Mempool
	mp := mempool.NewMempool()

	stateCache := persister.NewStateCache("./statedb")
	// 调度执行
	s := scheduler.NewScheduler(stateCache)

	//comWg := new(sync.WaitGroup)
	//ioWg := new(sync.WaitGroup)
	threadNum := 4
	runtime.GOMAXPROCS(threadNum)
	for i := 0; i < 10000; i++ { // 区块
		txChan := make(chan *config.Transaction, 2000)
		competingTxChan := make(chan *config.Transaction, 1000)
		ioTxChan := make(chan *config.Transaction, 1000)
		for j := 0; j < 100; j++ { // 每个区块中的交易个数
			txChan <- mp.GetTx()
			//competingTxChan <- mp.GetCompetingTx()
			//ioTxChan <- mp.GetIOTx()
		}
		close(txChan)
		close(ioTxChan)
		close(competingTxChan)
		wg.Add(threadNum)
		for k := 0; k < threadNum; k++ {
			go s.Run(stateCache, txChan, wg)
		}

		wg.Wait()
		//comWg.Add(threadNum / 2)
		//for k := 0; k < threadNum/2; k++ {
		//	go s.Run(stateCache, competingTxChan, comWg)
		//}
		//comWg.Wait()

		//ioWg.Add(threadNum)
		//for k := 0; k < threadNum; k++ {
		//	go s.Run(stateCache, ioTxChan, ioWg)
		//}
		//ioWg.Wait()
		stateCache.Commit()
		fmt.Println("Finished one block")
		//time.Sleep(5 * time.Second)
	}
}
