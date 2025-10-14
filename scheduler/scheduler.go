package scheduler

import (
	"Janus/config"
	"Janus/core"
	"Janus/persister"
	"runtime"
	"sync"
)

// Scheduler 负责调度交易执行
type Scheduler struct {
	stateCache *persister.StateCache
}

// NewScheduler 创建调度器
func NewScheduler(stateCache *persister.StateCache) *Scheduler {
	return &Scheduler{stateCache: stateCache}
}

// Run 执行一批交易
func (s *Scheduler) Run(cache *persister.StateCache, txChan chan *config.Transaction, wg *sync.WaitGroup) {
	defer wg.Done()
	runtime.LockOSThread()
	for tx := range txChan {
		core.ExecuteTransaction(cache, tx)
	}
}

func (s *Scheduler) RunComputingTx(cache *persister.StateCache, txChan chan *config.Transaction, wg *sync.WaitGroup) {
	defer wg.Done()
	runtime.LockOSThread()
	for tx := range txChan {
		core.ExecuteCompetingTransaction(cache, tx)
	}
}

func (s *Scheduler) RunIOTx(cache *persister.StateCache, txChan chan *config.Transaction, wg *sync.WaitGroup) {
	defer wg.Done()
	runtime.LockOSThread()
	for tx := range txChan {
		core.ExecuteIOTransaction(cache, tx)
	}
}
