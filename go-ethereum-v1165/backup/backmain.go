package backup

import (
	"Janus/mempool"
	"Janus/persister"
	"Janus/scheduler"
)

func runSep(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
	//occIOTxSlice := make([]chan *config.Transaction, 0)
	//occComputingTxSlice := make([]chan *config.Transaction, 0)
	//for j := 0; j < (txSum/2)/allThreadNum; j++ { // 每个区块中的交易个数
	//	txChan := make(chan *config.Transaction, chanLen)
	//	for i := 0; i < allThreadNum; i++ {
	//		txChan <- mp.GetIOTx()
	//		txChan <- mp.GetCompetingTx()
	//	}
	//	occTxSlice = append(occTxSlice, txChan)
	//	//fmt.Println("occTxSlice: ", occTxSlice)
	//}
	//
	//start := time.Now()
	//for _, txChan := range occTxSlice { // 等待同一批次运行完成
	//	wg := new(sync.WaitGroup)
	//	close(txChan)
	//	wg.Add(allThreadNum)
	//	for k := 0; k < allThreadNum; k++ {
	//		go s.Run(stateCache, txChan, wg, k)
	//	}
	//	wg.Wait()
	//}
	//
	//comWg := new(sync.WaitGroup)
	//ioWg := new(sync.WaitGroup)
	//for i := 0; i < blockSum; i++ { // 区块
	//	computingTxChan := make(chan *config.Transaction, chanLen)
	//	ioTxChan := make(chan *config.Transaction, chanLen)
	//	for j := 0; j < txSum/2; j++ { // 每个区块中的交易个数
	//		computingTxChan <- mp.GetCompetingTx()
	//		ioTxChan <- mp.GetIOTx()
	//	}
	//	close(ioTxChan)
	//	close(computingTxChan)
	//
	//	start := time.Now()
	//	comWg.Add(computingThreadNum)
	//	for k := 0; k < computingThreadNum; k++ {
	//		go s.Run(stateCache, computingTxChan, comWg, k)
	//	}
	//	comWg.Wait()
	//
	//	ioWg.Add(ioThreadNum)
	//	for k := 0; k < ioThreadNum; k++ {
	//		go s.Run(stateCache, ioTxChan, ioWg, k)
	//	}
	//	ioWg.Wait()
	//	stateCache.Commit()
	//	fmt.Printf("Finished %d block, TPS: %f\n", i, txSum/time.Since(start).Seconds())
	//	//time.Sleep(5 * time.Second)
	//}
}

//func runComputing(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
//	fmt.Println("Running computing threads")
//	time.Sleep(1 * time.Second)
//	comWg := new(sync.WaitGroup)
//	for i := 0; i < blockSum; i++ { // 区块
//		computingTxChan := make(chan *config.Transaction, chanLen)
//		for j := 0; j < txSum; j++ { // 每个区块中的交易个数
//			computingTxChan <- mp.GetCompetingTx()
//		}
//		close(computingTxChan)
//
//		start := time.Now()
//		comWg.Add(allThreadNum)
//		for k := 0; k < allThreadNum; k++ {
//			go s.Run(stateCache, computingTxChan, comWg, k)
//		}
//		comWg.Wait()
//		stateCache.Commit()
//		fmt.Printf("Finished %d block, TPS: %f\n", i, txSum/time.Since(start).Seconds())
//		//time.Sleep(1 * time.Second)
//	}
//}
//
//func runIO(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
//	fmt.Println("Running IO threads...")
//	ioWg := new(sync.WaitGroup)
//	for i := 0; i < blockSum; i++ { // 区块
//		ioTxChan := make(chan *config.Transaction, chanLen)
//		for j := 0; j < txSum; j++ { // 每个区块中的交易个数
//			ioTxChan <- mp.GetIOTx()
//		}
//		close(ioTxChan)
//
//		start := time.Now()
//		ioWg.Add(allThreadNum)
//		for k := 0; k < allThreadNum; k++ {
//			go s.Run(stateCache, ioTxChan, ioWg, k)
//		}
//		ioWg.Wait()
//		stateCache.Commit()
//		fmt.Printf("Finished %d block, time cost %f, TPS: %f\n", i, time.Since(start).Seconds(), txSum/time.Since(start).Seconds())
//		//time.Sleep(1 * time.Second)
//	}
//}
