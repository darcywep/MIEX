package Harmony

import (
	"Janus/plugin/Common"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type Harmony struct {
	statistics       *Common.Statistics
	blocks           []*Common.Block
	table            *HarmonyTable
	lockTable        *HarmonyLockTable
	enableInterBlock bool
	numThreads       int
	confirmExit      atomic.Int64
	stopFlag         atomic.Bool
	barrier          *sync.WaitGroup // 使用WaitGroup模拟barrier，或者使用其他同步机制
	counter          atomic.Int64
	pool             Common.ThreadPool // 线程池
}

func (h *Harmony) NewHarmony(
	blocks []*Common.Block,
	statistics *Common.Statistics,
	numThreads int,
	tablePartitions int,
	enableInterBlock bool,
) *Harmony {
	barrier := NewHarmonyBarrier(numThreads, func() {
		fmt.Println("batch complete")
	})

	harmony := &Harmony{
		blocks:           blocks,
		statistics:       statistics,
		barrier:          barrier,
		table:            NewHarmonyTable(tablePartitions),
		lockTable:        NewHarmonyLockTable(tablePartitions),
		enableInterBlock: enableInterBlock,
		numThreads:       numThreads,
	}

}

type HarmonyBarrier struct {
	wg           sync.WaitGroup
	onCompletion func()
}

func NewHarmonyBarrier(parties int, completion func()) *HarmonyBarrier {
	b := &HarmonyBarrier{onCompletion: completion}
	b.wg.Add(parties)
	return b
}

func (b *HarmonyBarrier) ArriveAndWait() {
	b.wg.Done()
	b.wg.Wait()
	if b.onCompletion != nil {
		b.onCompletion()
	}
}

type HarmonyExecutor struct {
	statistics       *Common.Statistics
	batchTxs         [][]*HarmonyTransaction
	table            *HarmonyTable
	lockTable        *HarmonyLockTable
	enableInterBlock bool
	numThreads       uint32
	confirmExit      *atomic.Int32
	stopFlag         *atomic.Bool
	barrier          *HarmonyBarrier
	counter          *atomic.Int32
	workerID         uint32
	batchIdx         uint32
}
