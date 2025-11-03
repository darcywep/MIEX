package schain

import (
	"Janus/config"
	"Janus/mempool"
	"Janus/persister"
	"Janus/scheduler"
	"testing"
)

func TestPeep(t *testing.T) {
	mp := mempool.NewMempool()
	if mp.ComputeTxs == nil {
		return
	}
	stateCache := persister.NewStateCache(config.JanusDBPath, config.Key2addrDBPath)
	// 调度执行
	s := scheduler.NewScheduler(stateCache)
	Peep(stateCache, mp, s)
}
