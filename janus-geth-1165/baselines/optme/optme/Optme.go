package optme

import (
	"fmt"
	"janus-geth-1165/baselines/common"
	janusConfig "janus-geth-1165/config"
	lvm "janus-geth-1165/core/evm"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

func (optme *OptME) OptmeTest() {
	fmt.Println("Ready to test OptME")
}

// OptMEEntry 表条目
type OptMEEntry struct {
	Value           string                         // 某个 Key 的 Value
	BlockIDGet      uint32                         // 最后读取该Key的区块id
	BlockIDPut      uint32                         // 最后写入该key的区块id
	ReservedGetTxs  map[*OptmeTransaction]struct{} // 当前区块对这个key的读预约次数
	ReservedPutNum  uint32                         // 当前区块对这个key的写预约次数
	NextReservedPut uint32                         // 下一个区块对这个key的写入预约次数
}

// OptMETable 表示执行表
type OptMETable struct {
	partitions int
	table      common.Table[OptMEEntry] // 每个key对应一个OptMEEntry
}

// NewOptMETable 创建新的OptME表
func NewOptMETable(partitions int) *OptMETable {
	return &OptMETable{
		partitions: partitions,
		table:      *common.NewTable[OptMEEntry](partitions),
	}
}

type OptME struct {
	Statistics *common.Statistics
	blocks     []*common.Block
	batches    [][]*OptmeTransaction
	acgs       []*AddressBasedConflictGraph

	numThreads     int
	table          *OptMETable
	enableParallel bool
	committedBlock atomic.Uint64
	pool           *common.ThreadPool
	blockIdx       uint64
	mtx            sync.Mutex
	cv             *sync.Cond
}

// NewOptME 创建新的OptME实例
func NewOptME(blocks []*common.Block, statistics *common.Statistics, numThreads int, tablePartitions int, enableParallel bool, levm *lvm.LEVM) *OptME {
	optme := &OptME{
		Statistics:     statistics,
		blocks:         blocks,
		numThreads:     numThreads,
		table:          NewOptMETable(tablePartitions),
		enableParallel: enableParallel,
		pool:           common.NewThreadPool(numThreads, levm),
	}
	optme.cv = sync.NewCond(&optme.mtx)
	return optme
}

func (optme *OptME) GetThreadPool() *common.ThreadPool {
	return optme.pool
}

// Start 启动OptME协议
func (optme *OptME) Start() {

	fmt.Println("OptME Ready to Start ...")

	txid := 0
	blockid := 0
	// 将块拆分为批次
	for _, block := range optme.blocks {

		//blockid++
		txs := block.GetTxs()
		batch := make([]*OptmeTransaction, 0, janusConfig.BlockSize)

		for i := 0; i < janusConfig.BlockSize; i++ {
			txid++
			txs[i].Txid = uint32(txid)
			optmeTx := NewOptmeTransaction(txs[i], uint32(blockid))
			batch = append(batch, optmeTx) // batch[][], 里面每个元素表示一个区块中的所有交易
		}

		acg := NewAddressBasedConflictGraph(optme.pool) // 地址冲突图
		optme.acgs = append(optme.acgs, acg)
		optme.batches = append(optme.batches, batch)
	}

	fmt.Println("OptME Ready to Run ...")
	optme.Run()
}

// hasContain 检查map中的key是否在目标集合中存在
func (o *OptME) hasContain(sourceMap map[string]string, targetSet map[string]struct{}) bool {
	for key := range sourceMap {
		if _, exists := targetSet[key]; exists {
			return true
		}
	}
	return false
}

// IntraEpochReordering 时期内重排序
func (optme *OptME) IntraEpochReordering(simulationResult []*OptmeTransaction, abortedTxs *[]*OptmeTransaction, txList *[]*OptmeTransaction) {

	fmt.Print("Ready to Run IntraEpochReordering... \n")

	// 构建ACG和回滚
	acg := NewAddressBasedConflictGraph(optme.pool) // 初始化acg
	fmt.Printf("thread pool cuncurrency degree: %d \n", acg.pool.ThreadNum)

	beginTime := time.Now()

	// 构建ACG
	acg.ParallelConstruct(simulationResult)
	//acg.Construct(simulationResult)
	constructTime := time.Now()
	constructDuration := constructTime.Sub(beginTime).Microseconds()
	fmt.Printf("Construct ACG time: %.2f ms \n", float64(constructDuration)/1000.0)

	// 分层排序
	acg.HierarchicalSort()
	sortTime := time.Now()
	sortDuration := sortTime.Sub(constructTime).Microseconds()
	fmt.Printf("Sort ACG time: %.2fms: \n", float64(sortDuration)/1000.0)

	// 重排序
	acg.Reorder()
	reorderTime := time.Now()
	reorderDuration := reorderTime.Sub(sortTime).Microseconds()
	fmt.Printf("Reorder ACG time: %.2fms: \n", float64(reorderDuration)/1000.0)

	//提取中止的交易和交易列表
	*abortedTxs = acg.extractAbortList()
	*txList = acg.extractTxsList()
}

// ReorderWithACG 使用ACG重排序交易
func (o *OptME) ReorderWithACG(acg *AddressBasedConflictGraph, simulationResult []*OptmeTransaction, abortedTxs *[]*OptmeTransaction) {

	//acg.ParallelConstruct(simulationResult)
	acg.Construct(simulationResult)

	beginTime := time.Now()

	var txList []*OptmeTransaction

	// 时期内重排序
	o.IntraEpochReorderingWithACG(acg, abortedTxs, &txList)

	//fmt.Printf("abortedTxs size = %d \n", len(*abortedTxs))

	// 并发提交并统计延迟
	for _, tx := range txList {
		latency := time.Since(tx.StartTime).Microseconds()
		o.Statistics.JournalCommit(uint32(latency))
	}

	// 统计整个重排序阶段的执行时间
	phaseTime := time.Since(beginTime).Microseconds()
	o.Statistics.JournalRollbackExecution(uint32(phaseTime))
}

// Reorder 重排序交易
func (optme *OptME) Reorder(simulationResult []*OptmeTransaction, abortedTxs *[]*OptmeTransaction) {

	fmt.Print("Ready to Serial Reorder... \n")

	////// 定义局部函数用于计算延迟
	//calcLatency := func(tx *OptmeTransaction) int64 {
	//	return time.Since(tx.StartTime).Microseconds()
	//}

	beginTime := time.Now()

	//// 构建ACG和回滚
	var txList []*OptmeTransaction
	optme.IntraEpochReordering(simulationResult, abortedTxs, &txList)

	//// 并发提交，统计延时
	//for _, tx := range txList {
	//	latency := calcLatency(tx)
	//	optme.statistics.JournalCommit(uint32(latency))
	//}
	//
	phaseTime := time.Since(beginTime).Microseconds()
	optme.Statistics.JournalRollbackExecution(uint32(phaseTime)) // 该阶段耗时
	//fmt.Printf("phaseTime = %v\n", uint32(phaseTime))
}

// Run 运行OptME协议
func (optme *OptME) Run() {
	// 执行每个批次
	fmt.Println("Go in OptME Run function ...")
	for i, batch := range optme.batches {

		acg := optme.acgs[i] // addressConflictGraph
		blockid := batch[0].Blockid

		//fmt.Printf("Batch size: %d \n", len(batch))
		//fmt.Printf("txList size: %d \n", len(acg.txList))

		var schedules [][]*OptmeTransaction
		var abortedTxs []*OptmeTransaction

		optme.Simulate(batch, blockid) // 模拟(预)执行

		if optme.enableParallel {
			//fmt.Printf("Parallelization: %t \n", optme.enableParallel)
			optme.ReorderWithACG(acg, batch, &abortedTxs)
		} else {
			//fmt.Printf("Parallelization: %t \n", optme.enableParallel)
			optme.Reorder(batch, &abortedTxs)
		}
		optme.ParallelExecute(&schedules, abortedTxs)
		optme.Statistics.JournalBlock()

		optme.pool.ResetEVM()
	}
}

// InterEpochReordering 跨时期重排序
func (o *OptME) InterEpochReordering(schedules *[][]*OptmeTransaction, abortedTxs []*OptmeTransaction) {
	// 重新调度中止的交易
	epochMap := make([]map[string]struct{}, 0)

	for _, tx := range abortedTxs {
		epoch := 0
		// 找到第一个没有冲突的时期
		for epoch < len(epochMap) && (o.hasContain(tx.LocalGet, epochMap[epoch]) || o.hasContain(tx.LocalPut, epochMap[epoch])) {
			epoch++
		}

		// 如果需要新的时期，扩展切片
		if epoch >= len(epochMap) {
			epochMap = append(epochMap, make(map[string]struct{}))
			*schedules = append(*schedules, make([]*OptmeTransaction, 0))
		}

		// 将交易添加到对应的时期
		(*schedules)[epoch] = append((*schedules)[epoch], tx)

		// 更新时期映射，添加写操作的key
		for key := range tx.LocalPut {
			epochMap[epoch][key] = struct{}{}
		}
	}
}

func (optme *OptME) ReExecute(tx *OptmeTransaction, levm *lvm.LEVM) int {
	tx.Execute(levm)
	return 1
}

// ParallelExecute 并行执行交易
func (optme *OptME) ParallelExecute(schedules *[][]*OptmeTransaction, abortedTxs []*OptmeTransaction) {
	// 定义局部函数模拟宏
	//calcLatency := func(tx *OptmeTransaction) int64 {
	//	return time.Since(tx.StartTime).Microseconds()
	//}
	//
	//calcPhaseTime := func(start time.Time) int64 {
	//	return time.Since(start).Microseconds()
	//}
	//
	////log.Infof("ReExecute block %d", o.blockIdx)
	//beginTime := time.Now()

	// 重新调度交易
	optme.InterEpochReordering(schedules, abortedTxs)

	// 并发重新执行
	for _, schedule := range *schedules {
		var wg sync.WaitGroup
		var futures []chan int // 用于错误处理

		for _, tx := range schedule {
			wg.Add(1)
			resultChan := make(chan int, 1)
			futures = append(futures, resultChan)

			optme.pool.Enqueue(func(levm *lvm.LEVM) {
				defer wg.Done()
				defer close(resultChan)

				err := optme.ReExecute(tx, levm)
				if err != 1 {
					resultChan <- err
					return
				}
				optme.Finalize(tx)
				resultChan <- 1
			})

			//optme.pool.Submit(func() {
			//	defer wg.Done()
			//	defer close(resultChan)
			//
			//	err := optme.ReExecute(tx)
			//	if err != 1 {
			//		resultChan <- err
			//		return
			//	}
			//	optme.Finalize(tx)
			//	resultChan <- 1
			//})

			// 统计执行信息
			//optme.statistics.JournalExecute()
			//optme.statistics.JournalCommit(calcLatency(tx))
			//optme.statistics.JournalRollback(tx.CountOverheads())
		}

		wg.Wait()

		// 检查执行结果（可选）
		for _, future := range futures {
			if err := <-future; err != 1 {
				//log.Errorf("Transaction execution failed: %v", err)
			}
		}
	}

	//optme.statistics.JournalReExecution(calcPhaseTime(beginTime))
	//log.Infof("ReExecute block %d done", o.blockIdx)
}

// IntraEpochReorderingWithACG 使用ACG进行时期内重排序
func (o *OptME) IntraEpochReorderingWithACG(acg *AddressBasedConflictGraph, abortedTxs *[]*OptmeTransaction, txList *[]*OptmeTransaction) {

	// 分层排序
	acg.HierarchicalSort()

	// 重排序
	acg.Reorder()

	// 提取中止的交易和交易列表
	*abortedTxs = acg.extractAbortList()
	*txList = acg.extractTxsList()
}

// ReserveGet 检查读取冲突
func (opt *OptMETable) ReserveGet(tx *OptmeTransaction, key string) {
	opt.table.Put(key, func(entry *OptMEEntry) {
		// 没有写后读冲突
		if entry.BlockIDPut == 0 || entry.BlockIDPut == tx.Blockid { // 读和写属一个区块
			entry.BlockIDGet = max(entry.BlockIDGet, tx.Blockid) //
		} else {
			fmt.Printf("中止交易.......\n")
			tx.Aborted.Store(true) //
		}
	})
}

// ReservePut 保留放置操作
func (opt *OptMETable) ReservePut(tx *OptmeTransaction, key string) {
	opt.table.Put(key, func(entry *OptMEEntry) {

		if entry.BlockIDPut == 0 { // 第一次写入
			entry.BlockIDPut = tx.Blockid
			entry.ReservedPutNum = 1
		} else if entry.BlockIDPut == tx.Blockid {
			entry.ReservedPutNum++
		} else if entry.BlockIDPut < tx.Blockid {
			// 存储下一个保留的放置
			entry.NextReservedPut++
		}
	})
}

// Simulate 模拟交易执行
func (optme *OptME) Simulate(batch []*OptmeTransaction, blockid uint32) {
	//fmt.Println("开始模拟执行区块 %d", blockid)

	var wg sync.WaitGroup
	for _, tx := range batch {
		wg.Add(1)

		optme.pool.Enqueue(func(levm *lvm.LEVM) {
			defer wg.Done()
			tx.StartTime = time.Now() // 开始计时

			tx.Execute(levm) // 执行交易

			//fmt.Printf("tx type = %d \n", tx.EthTx.TxType)

			// 实施读写操作，从本地存储读取 LocalGet
			var keys []string
			for key, value := range tx.Tx.Vertex.ReadKeys {
				keys = append(keys, key)

				//fmt.Printf("readKey = %s \n", key)

				optme.table.ReserveGet(tx, key) // 检查所有的读，是否有写后读冲突
				tx.LocalGet[key] = value
			}

			for key, value := range tx.Tx.Vertex.WriteKeys {

				//fmt.Printf("writeKey = %s \n", key)

				keys = append(keys, key)
				optme.table.ReservePut(tx, key) // 登记所有的写
				tx.LocalPut[key] = value
			}

			optme.Statistics.JournalExecute()
			//optme.statistics.JournalOverheads(tx.Tx.Cost)
		})
	}

	// 等待所有goroutine完成
	wg.Wait()
	//fmt.Printf("模拟执行区块：%d 完毕, 执行次数：%d \n", blockid, optme.Statistics.ExecCount.Load())
}

// Stop 停止OptME协议
func (optme *OptME) Stop() {
	optme.pool.Shutdown()
	log.Info("OptME stopped")
}

func (optme *OptME) Finalize(tx *OptmeTransaction) {
	for key := range tx.LocalPut {
		optme.table.table.Put(key, func(entry *OptMEEntry) {
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
