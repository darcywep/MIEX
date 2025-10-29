package Optme

import (
	"Janus/plugin/Common"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/log"
)

// OptMEEntry 表条目
type OptMEEntry struct {
	Value           string                         // 某个 Key 的 Value
	BlockIDGet      uint64                         // 最后读取该Key的区块号
	ReservedGetTxs  map[*OptmeTransaction]struct{} //
	BlockIDPut      uint64
	ReservedPutNum  uint64
	NextReservedPut uint64
}

// OptMETable 表示执行表
type OptMETable struct {
	partitions int
	entries    []*sync.Map
}

func NewOptMETable(partitions int) *OptMETable {
	entries := make([]*sync.Map, partitions)

	for i := range entries {
		entries[i] = &sync.Map{}
	}
	return &OptMETable{
		partitions: partitions,
		entries:    entries,
	}
}

type OptME struct {
	statistics *Statistics
	blocks     []*Common.Block
	batches    [][]*OptmeTransaction
	acgs       []*AddressBasedConflictGraph

	numThreads     int
	table          *OptMETable
	enableParallel bool
	committedBlock atomic.Uint64
	pool           *ThreadPool
	blockIdx       uint64
	mtx            sync.Mutex
	cv             *sync.Cond
}

// NewOptME 创建新的OptME实例
func NewOptME(blocks []*Common.Block, statistics *Statistics, numThreads int, tablePartitions int, enableParallel bool) *OptME {
	optme := &OptME{
		statistics:     statistics,
		blocks:         blocks,
		numThreads:     numThreads,
		table:          NewOptMETable(tablePartitions),
		enableParallel: enableParallel,
		pool:           NewThreadPool(numThreads),
	}
	optme.cv = sync.NewCond(&optme.mtx)
	return optme
}

// Start 启动OptME协议
func (optme *OptME) Start() {
	log.Info("OptME started")

	// 将块拆分为批次
	for _, block := range optme.blocks {
		txs := block.GetTxs()
		batch := make([]*OptmeTransaction, 0, len(txs))

		for _, tx := range txs {
			blockid := 0
			optmeTx := NewOptmeTransaction(tx, uint32(blockid))
			batch = append(batch, optmeTx)
		}

		acg := NewAddressBasedConflictGraph(optme.pool)
		optme.acgs = append(optme.acgs, acg)
		optme.batches = append(optme.batches, batch)
	}

	optme.Run()
}

// Run 运行OptME协议
func (optme *OptME) Run() {
	// 执行每个批次
	for i, batch := range optme.batches {
		acg := optme.acgs[i]
		var schedules [][]*OptmeTransaction
		var abortedTxs []*OptmeTransaction

		optme.Simulate(batch)
		if optme.enableParallel {
			optme.ReorderWithACG(acg, &abortedTxs)
		} else {
			optme.Reorder(batch, &abortedTxs)
		}
		optme.ParallelExecute(&schedules, abortedTxs)
		optme.statistics.JournalBlock()
		//log.Infof("Block %d finalize done", o.blockIdx)
	}
}

// ReserveGet 检查读取冲突
func (optme *OptMETable) ReserveGet(tx *OptmeTransaction, key string) {
	// 检查：如果已经有其他区块的写操作，则中止当前读操作
	if optmeEntry.BlockIDPut == 0 || optmeEntry.BlockIDPut == tx.BlockID {
		// 允许读取
	} else {
		tx.Aborted.Store(true) // 中止交易
	}
}

// ReservePut 管理写入冲突
func (optme *OptMETable) ReservePut(tx *OptmeTransaction, key string) {
	if optmeEntry.BlockIDPut == 0 {
		// 第一个写入者
		optmeEntry.BlockIDPut = tx.BlockID
		optmeEntry.ReservedPutNum = 1
	} else if optmeEntry.BlockIDPut == tx.BlockID {
		// 同一区块的多个写入
		optmeEntry.ReservedPutNum++
	} else if optmeEntry.BlockIDPut < tx.BlockID {
		// 后续区块的写入请求
		optmeEntry.NextReservedPut++
	}
}

func (optme *OptME) Simulate(batch []*OptmeTransaction) {
	for _, tx := range batch {
		// 读取时调用 ReserveGet
		optme.table.ReserveGet(tx, key)
		// 写入时调用 ReservePut
		optme.table.ReservePut(tx, key)
	}
}

// Stop 停止OptME协议
func (optme *OptME) Stop() {
	o.pool.Shutdown()
	log.Info("OptME stopped")
}

func (optme *OptME) Finalize(tx *OptmeTransaction) {
	for key := range tx.LocalPut {
		optme.table.Put(key, func(entry *OptMEEntry) {
			if entry.BlockIDPut == tx.Blockid {
				entry.ReservedPutNum--
				if entry.ReservedPutNum == 0 {
					// 处理下一个区块的写入
					if entry.NextReservedPut > 0 {
						entry.BlockIDPut++
						entry.ReservedPutNum = entry.NextReservedPut
						entry.NextReservedPut = 0
					} else {
						entry.BlockIDPut = 0 // 重置
					}
				}
			}
		})
	}
}
