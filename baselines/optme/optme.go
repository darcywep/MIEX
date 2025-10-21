package optme

import (
	"fmt"
	"sync"
	"time"
)

// -----------------------------
// 简化/占位类型（请用项目真实实现替换）
// -----------------------------

// Statistics 占位：记录若干计数/耗时（非常简化）
type Statistics struct {
	mu sync.Mutex

	ExecCount      int
	CommitCount    int
	RollbackCount  int
	JournaledBlock int
}

func OptmeTest() {
	fmt.Printf("Here is OptmeTest \n")
}

func (s *Statistics) JournalExecute() { s.mu.Lock(); s.ExecCount++; s.mu.Unlock() }
func (s *Statistics) JournalCommit(latencyMicros int64) {
	s.mu.Lock()
	s.CommitCount++
	_ = latencyMicros
	s.mu.Unlock()
}
func (s *Statistics) JournalRollback(overheads int) {
	s.mu.Lock()
	s.RollbackCount += overheads
	s.mu.Unlock()
}
func (s *Statistics) JournalBlock()                  { s.mu.Lock(); s.JournaledBlock++; s.mu.Unlock() }
func (s *Statistics) JournalOverheads(int)           {}
func (s *Statistics) JournalReExecution(int64)       {}
func (s *Statistics) JournalRollbackExecution(int64) {}

// Block 占位：一个 Block 包含若干待执行的“事务”，每个事务由外部创建并填充 ReadSet/WriteSet
type Block struct {
	Txs []*OptMETransaction
}

// -----------------------------
// Core data structures
// -----------------------------

// OptMETransaction 表示单笔事务（简化）：
// - ReadSet / WriteSet 为事务的静态（或上层构造时）已知集合
// - Execute() 模拟业务消耗（调用 get/set handler）
type OptMETransaction struct {
	ID       uint64
	BlockID  uint64
	Sequence uint64

	Committed bool
	Aborted   bool

	StartTime time.Time

	// 上层需要填充这两个集合，Execute() 会调用 handler
	ReadSet  []string
	WriteSet []string

	// 本地读写缓存（模拟）
	LocalGet map[string]string
	LocalPut map[string]string

	// handlers（由 OptME 在不同阶段安装）
	GetHandler func(keys []string)
	SetHandler func(keys []string, value string)

	// cost 模拟执行时间/开销（可选）
	Cost time.Duration
}

// CountOverheads 简化计数：返回一个代表“回滚开销”的整数
func (tx *OptMETransaction) CountOverheads() int {
	// 简化：每次回滚认为开销 1
	if tx.Aborted {
		return 1
	}
	return 0
}

// Execute 调用已安装的 handlers，并模拟执行时间
func (tx *OptMETransaction) Execute() {
	// 记录开始时间（上层会再次设置 start_time）
	if tx.GetHandler != nil {
		tx.GetHandler(tx.ReadSet)
	}
	if tx.SetHandler != nil {
		// 写 handler 传入默认 value（演示）
		tx.SetHandler(tx.WriteSet, fmt.Sprintf("val_tx_%d", tx.ID))
	}
	// 模拟一些计算/等待
	if tx.Cost > 0 {
		time.Sleep(tx.Cost)
	}
}

// -----------------------------
// OptME 表（并发安全）
// -----------------------------

type OptMEEntry struct {
	Value           string
	BlockIDGet      uint64
	ReservedGetTxs  map[uint64]struct{} // 存 tx.ID
	BlockIDPut      uint64
	ReservedPutNum  int
	NextReservedPut int
}

// callback style 的 Put/Get，类似 C++ 版的 lambda 修改
type EntryMutator func(entry *OptMEEntry)

type OptMETable struct {
	mu    sync.RWMutex
	table map[string]*OptMEEntry
}

func NewOptMETable() *OptMETable {
	return &OptMETable{
		table: make(map[string]*OptMEEntry),
	}
}

// Put 提供一个对 entry 的原子操作（创建或修改）
func (t *OptMETable) Put(key string, mutator EntryMutator) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ent, ok := t.table[key]
	if !ok {
		ent = &OptMEEntry{
			Value:          "",
			BlockIDGet:     0,
			ReservedGetTxs: make(map[uint64]struct{}),
			BlockIDPut:     0,
			ReservedPutNum: 0,
		}
		t.table[key] = ent
	}
	mutator(ent)
}

// Get 提供对 entry 只读访问（回调读取）
func (t *OptMETable) Get(key string, reader func(entry *OptMEEntry)) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	ent, ok := t.table[key]
	if !ok {
		// 传入空的临时 entry，避免 nil
		tmp := &OptMEEntry{Value: ""}
		reader(tmp)
		return
	}
	reader(ent)
}

// ReserveGet / ReservePut: 与 C++ 行为类似的保守预留
func (t *OptMETable) ReserveGet(tx *OptMETransaction, k string) {
	t.Put(k, func(entry *OptMEEntry) {
		// 如果当前有 put 来自不同的 block，则读失败（WAR）
		if entry.BlockIDPut == 0 || entry.BlockIDPut == tx.BlockID {
			if tx.BlockID > entry.BlockIDGet {
				entry.BlockIDGet = tx.BlockID
			}
			// track reserved get tx ids
			entry.ReservedGetTxs[tx.ID] = struct{}{}
		} else {
			// 冲突 => abort tx
			tx.Aborted = true
		}
	})
}

func (t *OptMETable) ReservePut(tx *OptMETransaction, k string) {
	t.Put(k, func(entry *OptMEEntry) {
		if entry.BlockIDPut == 0 {
			entry.BlockIDPut = tx.BlockID
			entry.ReservedPutNum = 1
		} else if entry.BlockIDPut == tx.BlockID {
			entry.ReservedPutNum++
		} else if entry.BlockIDPut < tx.BlockID {
			entry.NextReservedPut++
		}
	})
}

// ReleasePut 在 Finalize 时调用，用来调整 reserved_put_num / next_reserved_put
func (t *OptMETable) ReleasePut(tx *OptMETransaction, k string) {
	t.Put(k, func(entry *OptMEEntry) {
		if entry.BlockIDPut == tx.BlockID {
			entry.ReservedPutNum--
			if entry.ReservedPutNum == 0 {
				// promote next_reserved_put if exists
				if entry.NextReservedPut > 0 {
					entry.BlockIDPut++ // 简化：增加 block id (示意)
					entry.ReservedPutNum = entry.NextReservedPut
					entry.NextReservedPut = 0
				} else {
					entry.BlockIDPut = 0
				}
			} else if entry.ReservedPutNum < 0 {
				// 应该不会出现，但记录错误
				entry.ReservedPutNum = 0
			}
		}
	})
}

// PutValue 直接写入 value（用于 ReExecute 阶段写回）
func (t *OptMETable) PutValue(k string, value string) {
	t.Put(k, func(entry *OptMEEntry) {
		entry.Value = value
	})
}

// -----------------------------
// AddressBasedConflictGraph (简化实现)
// -----------------------------
//
// 该版本将构造一个简单的基于 address(key) 的冲突图，做两件事：
// - 检查 write-write 或 write-read 冲突（不同事务写同一 key 或一个写在另一个读之后导致 abort）
// - 提供 reorder 输出：非 aborted 的 tx 按照输入顺序返回
//
// 真实系统里 ACG 更复杂（层次化排序、拓扑等），这里保留接口与简单语义。
// -----------------------------

type AddressBasedConflictGraph struct {
	// 这里只保存 tx 列表与部分结果
	txList    []*OptMETransaction
	aborted   []*OptMETransaction
	addresses map[string][]*OptMETransaction // key -> tx list (按到达顺序)
}

func NewACG() *AddressBasedConflictGraph {
	return &AddressBasedConflictGraph{
		txList:    make([]*OptMETransaction, 0),
		aborted:   make([]*OptMETransaction, 0),
		addresses: make(map[string][]*OptMETransaction),
	}
}

// parallel_construct / construct 的简化：遍历 simulation_result，检测简单冲突
func (acg *AddressBasedConflictGraph) ParallelConstruct(simulation []*OptMETransaction) {
	// 不真正并行，直接检测
	for _, tx := range simulation {
		conflict := false
		// check write-write with already registered write owners
		for _, w := range tx.WriteSet {
			if owners, ok := acg.addresses[w]; ok {
				// 存在写入者，冲突（简化：任何已有写者导致冲突）
				for _, owner := range owners {
					// 若 owner 来自不同 block 或不同 tx，则判冲突
					if owner.BlockID != tx.BlockID {
						conflict = true
						break
					}
				}
			}
			if conflict {
				break
			}
		}
		if conflict {
			tx.Aborted = true
			acg.aborted = append(acg.aborted, tx)
			continue
		}
		// 否则注册 tx 到地址表（只按 key）
		acg.txList = append(acg.txList, tx)
		for _, r := range tx.ReadSet {
			acg.addresses[r] = append(acg.addresses[r], tx)
		}
		for _, w := range tx.WriteSet {
			acg.addresses[w] = append(acg.addresses[w], tx)
		}
	}
}

func (acg *AddressBasedConflictGraph) HierarchicalSort() {
	// 简化实现：不改变顺序
}

func (acg *AddressBasedConflictGraph) Reorder() {
	// 简化实现：不改变顺序
}

func (acg *AddressBasedConflictGraph) ExtractAbortedTxs() []*OptMETransaction {
	return acg.aborted
}

func (acg *AddressBasedConflictGraph) ExtractTxList() []*OptMETransaction {
	return acg.txList
}

// -----------------------------
// OptME 主类实现
// -----------------------------

type OptME struct {
	statistics     *Statistics
	blocks         []*Block
	batches        [][]*OptMETransaction
	acgs           []*AddressBasedConflictGraph
	numThreads     int
	table          *OptMETable
	enableParallel bool

	committedBlock uint64

	// goroutine 控制
	wg sync.WaitGroup

	// 控制变量
	blockIdx int
	mu       sync.Mutex
	cv       *sync.Cond
}

func NewOptME(blocks []*Block, stats *Statistics, numThreads int, enableParallel bool) *OptME {
	m := &OptME{
		statistics:     stats,
		blocks:         blocks,
		batches:        make([][]*OptMETransaction, 0),
		acgs:           make([]*AddressBasedConflictGraph, 0),
		numThreads:     numThreads,
		table:          NewOptMETable(),
		enableParallel: enableParallel,
		blockIdx:       0,
	}
	m.cv = sync.NewCond(&m.mu)
	return m
}

// Start: 构造 batches、acgs 占位并 Run
func (o *OptME) Start() {
	// 将 block 中的事务转换为 OptMETransaction（通常 caller 已经传入 OptMETransaction）
	// 这里假设 blocks 中的 Tx 已经是 OptMETransaction（或包装好的）
	for i := 0; i < len(o.blocks); i++ {
		block := o.blocks[i]
		batch := make([]*OptMETransaction, 0, len(block.Txs))
		for _, tx := range block.Txs {
			// 给 tx 安装默认 handler（simulate 阶段会覆盖）
			if tx.LocalGet == nil {
				tx.LocalGet = make(map[string]string)
			}
			if tx.LocalPut == nil {
				tx.LocalPut = make(map[string]string)
			}
			batch = append(batch, tx)
		}
		o.acgs = append(o.acgs, NewACG())
		o.batches = append(o.batches, batch)
	}
	o.Run()
}

// Run: 顺序执行每个 batch：Simulate -> Reorder -> ParallelExecute -> finalize block
func (o *OptME) Run() {
	for i := 0; i < len(o.batches); i++ {
		o.blockIdx = i + 1
		batch := o.batches[i]
		acg := o.acgs[i]
		schedules := make([][]*OptMETransaction, 0)
		abortedTxs := make([]*OptMETransaction, 0)

		o.Simulate(batch)
		if o.enableParallel {
			o.ReorderWithACG(acg, &abortedTxs)
		} else {
			o.Reorder(batch, &abortedTxs)
		}
		o.ParallelExecute(&schedules, abortedTxs)
		o.statistics.JournalBlock()
		// block 完成
	}
}

// Simulate: 并发执行 batch 中所有 tx 的 Execute（安装 local handlers）
func (o *OptME) Simulate(batch []*OptMETransaction) {
	o.blockIdx++
	var wg sync.WaitGroup
	wg.Add(len(batch))
	for _, tx := range batch {
		// install handlers for local simulation:
		tx.GetHandler = func(keys []string) {
			// local read: reserve get and put empty value into local_get
			for _, k := range keys {
				o.table.ReserveGet(tx, k)
				tx.LocalGet[k] = "" // simulate read result
			}
		}
		tx.SetHandler = func(keys []string, value string) {
			for _, k := range keys {
				o.table.ReservePut(tx, k)
				tx.LocalPut[k] = value
			}
		}

		tx.StartTime = time.Now()
		// run concurrently
		go func(t *OptMETransaction) {
			defer wg.Done()
			if t.Aborted {
				// 如果已经被标记为 aborted 就直接 skip（理论上少见）
				return
			}
			t.Execute()
			o.statistics.JournalExecute()
			o.statistics.JournalOverheads(t.CountOverheads())
		}(tx)
	}
	wg.Wait()
}

// Reorder: 不使用 ACG 的简化重排（以输入顺序为准），并产生 tx_list + aborted_txs
func (o *OptME) Reorder(simulationResult []*OptMETransaction, aborted *[]*OptMETransaction) {
	// 记录开始时间（用于 stats）
	begin := time.Now()
	txList := make([]*OptMETransaction, 0)
	for _, tx := range simulationResult {
		if tx.Aborted {
			*aborted = append(*aborted, tx)
			continue
		}
		txList = append(txList, tx)
	}
	// commit these txs concurrently (simulate commit latency)
	for _, tx := range txList {
		lat := time.Since(tx.StartTime).Microseconds()
		o.statistics.JournalCommit(lat)
	}
	o.statistics.JournalRollbackExecution(time.Since(begin).Microseconds())
}

// ReorderWithACG: 使用 AddressBasedConflictGraph 的重排流程
func (o *OptME) ReorderWithACG(acg *AddressBasedConflictGraph, aborted *[]*OptMETransaction) {
	begin := time.Now()
	// 构造 acg（简化实现）
	all := o.batches[o.blockIdx-1]
	acg.ParallelConstruct(all)

	// hierarchical sort + reorder (占位)
	acg.HierarchicalSort()
	acg.Reorder()

	// extract results
	extractedAborted := acg.ExtractAbortedTxs()
	extractedTxList := acg.ExtractTxList()

	*aborted = append(*aborted, extractedAborted...)

	// commit extractedTxList
	for _, tx := range extractedTxList {
		lat := time.Since(tx.StartTime).Microseconds()
		o.statistics.JournalCommit(lat)
	}
	o.statistics.JournalRollbackExecution(time.Since(begin).Microseconds())
}

// InterEpochReordering: 将 abortedTxs 重排到多个 epoch (schedules)
func (o *OptME) InterEpochReordering(schedules *[][]*OptMETransaction, aborted []*OptMETransaction) {
	// epoch_map: 每个 epoch 维护一组已经写入的 key（unordered_set）
	epochMap := make([]map[string]struct{}, 0)
	for _, tx := range aborted {
		epoch := 0
		for {
			if epoch >= len(epochMap) {
				// new epoch
				epochMap = append(epochMap, make(map[string]struct{}))
				*schedules = append(*schedules, make([]*OptMETransaction, 0))
			}
			// check overlap between tx's read/write set and epochMap[epoch]
			conflict := false
			for k := range tx.LocalGet {
				if _, ok := epochMap[epoch][k]; ok {
					conflict = true
					break
				}
			}
			if !conflict {
				for k := range tx.LocalPut {
					if _, ok := epochMap[epoch][k]; ok {
						conflict = true
						break
					}
				}
			}
			if !conflict {
				// place tx into this epoch
				(*schedules)[epoch] = append((*schedules)[epoch], tx)
				// add tx's put keys to epoch map
				for k := range tx.LocalPut {
					epochMap[epoch][k] = struct{}{}
				}
				break
			}
			epoch++
		}
	}
}

// ParallelExecute: 对每个 epoch 的 schedule 并发地 ReExecute + Finalize
func (o *OptME) ParallelExecute(schedules *[][]*OptMETransaction, aborted []*OptMETransaction) {
	begin := time.Now()
	// create schedules from aborted txs
	o.InterEpochReordering(schedules, aborted)

	// iterate schedules (each epoch)
	for _, sched := range *schedules {
		var wg sync.WaitGroup
		wg.Add(len(sched))
		for _, tx := range sched {
			go func(t *OptMETransaction) {
				defer wg.Done()
				o.ReExecute(t)
				o.Finalize(t)
				lat := time.Since(t.StartTime).Microseconds()
				o.statistics.JournalExecute()
				o.statistics.JournalCommit(lat)
				o.statistics.JournalRollback(t.CountOverheads())
			}(tx)
		}
		wg.Wait()
	}
	o.statistics.JournalReExecution(time.Since(begin).Microseconds())
}

// Finalize: 提交 tx、释放 table 上的 reserved_put
func (o *OptME) Finalize(tx *OptMETransaction) {
	tx.Committed = true
	// release reserved put entries for each written key
	for k := range tx.LocalPut {
		o.table.ReleasePut(tx, k)
	}
}

// ReExecute: 读取公共表并写回（fallback）
func (o *OptME) ReExecute(tx *OptMETransaction) {
	// 安装 fallback handlers：从公共表读取并写回
	tx.GetHandler = func(keys []string) {
		for _, k := range keys {
			o.table.Get(k, func(entry *OptMEEntry) {
				// copy value 回 local_get
				tx.LocalGet[k] = entry.Value
			})
		}
	}
	tx.SetHandler = func(keys []string, value string) {
		for _, k := range keys {
			o.table.PutValue(k, value)
			tx.LocalPut[k] = value
		}
	}
	// 执行
	tx.Execute()
}

// -----------------------------
// Usage helper: 构造简单事务 / block 示例
// -----------------------------

// NewTx 是一个帮助函数：快速构造一个事务（指定 id, blockid, read/write keys）
func NewTx(id, blockID uint64, reads, writes []string, cost time.Duration) *OptMETransaction {
	return &OptMETransaction{
		ID:       id,
		BlockID:  blockID,
		ReadSet:  reads,
		WriteSet: writes,
		LocalGet: make(map[string]string),
		LocalPut: make(map[string]string),
		Cost:     cost,
	}
}

// NewBlockFromTxs wrap transactions into Block
func NewBlockFromTxs(txs ...*OptMETransaction) *Block {
	return &Block{Txs: txs}
}
