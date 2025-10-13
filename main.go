package main

import (
	"Janus/config"
	"Janus/mempool"
	"Janus/persister"
	"Janus/scheduler"
	"fmt"
	"runtime"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	allThreadNum       = 4
	ioThreadNum        = 1
	competingThreadNum = 3
	blockSum           = 10000 // 执行多少个区块
	chanLen            = 2000  // 每个区块有多少笔交易
	txSum              = 200   // 每个区块有多少笔交易
)

func runAll(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
	wg := new(sync.WaitGroup)
	runtime.GOMAXPROCS(allThreadNum)
	for i := 0; i < blockSum; i++ { // 区块
		txChan := make(chan *config.Transaction, chanLen)

		for j := 0; j < txSum; j++ { // 每个区块中的交易个数
			txChan <- mp.GetTx()
		}
		close(txChan)
		wg.Add(allThreadNum)
		for k := 0; k < allThreadNum; k++ {
			go s.Run(stateCache, txChan, wg)
		}
		wg.Wait()
		stateCache.Commit()
		fmt.Printf("Finished %d block", i)
		//time.Sleep(5 * time.Second)
	}
}

func runSep(stateCache *persister.StateCache, mp *mempool.Mempool, s *scheduler.Scheduler) {
	comWg := new(sync.WaitGroup)
	ioWg := new(sync.WaitGroup)
	runtime.GOMAXPROCS(allThreadNum)
	for i := 0; i < blockSum; i++ { // 区块
		competingTxChan := make(chan *config.Transaction, chanLen)
		ioTxChan := make(chan *config.Transaction, chanLen)
		for j := 0; j < txSum; j++ { // 每个区块中的交易个数
			competingTxChan <- mp.GetCompetingTx()
			ioTxChan <- mp.GetIOTx()
		}
		close(ioTxChan)
		close(competingTxChan)

		comWg.Add(competingThreadNum)
		for k := 0; k < competingThreadNum; k++ {
			go s.Run(stateCache, competingTxChan, comWg)
		}
		comWg.Wait()

		ioWg.Add(ioThreadNum)
		for k := 0; k < ioThreadNum; k++ {
			go s.Run(stateCache, ioTxChan, ioWg)
		}
		ioWg.Wait()
		stateCache.Commit()
		fmt.Printf("Finished %d block", i)
		//time.Sleep(5 * time.Second)
	}
}

func main() {
	key2AddrDB, err := leveldb.OpenFile("./key2addrDB", &opt.Options{
		BlockCacheCapacity: 0, // 禁用 block cache
		WriteBuffer:        0, // 禁用写缓冲
		Strict:             opt.DefaultStrict,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer key2AddrDB.Close()

	mp := mempool.NewMempool()
	if mp.ComputeTxs == nil {
		return
	}

	//stateCache := persister.NewStateCache("./statedb")
	//// 调度执行
	//s := scheduler.NewScheduler(stateCache)
	//runAll(stateCache, mp, s)
	//runSep(stateCache, mp, s)
}
