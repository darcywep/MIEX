package OptME

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ----------------------
// 依赖说明（替换为你自己的实现）:
// - Block struct must have GetTxs returning tx wrappers that include underlying Transaction data
// - Table[K, OptMEEntry] needs Put/Get implementations; below is a placeholder interface used by OptMETable
// - Exec, hasContain, etc. are placeholders
// ----------------------

// Placeholder types and functions (replace with real ones)
type BlockPtr = *Block

// OptMETransaction corresponds to C++ struct OptMETransaction
type OptMETransaction struct {
	Transaction   // embed or compose your Transaction type
	id            uint64
	blockID       uint64
	sequence      uint64
	committedFlag uint32
	abortedFlag   uint32
	startTime     time.Time

	localGetMap map[string]string
	localPutMap map[string]string

	// For Initialize(path) where C++ used m_tx etc.
	m_tx *struct {
		m_rootVertex struct {
			allReadSet  map[string]struct{}
			allWriteSet map[string]struct{}
			m_cost      int
		}
	}

	// handlers (callbacks) similar to C++ getHandler/setHandler
	getHandler func(readSet map[string]struct{})
	setHandler func(writeSet map[string]struct{}, value string)
}

// Methods corresponding to C++ setters/getters
func (t *OptMETransaction) Execute() {
	log.Printf("Execute transaction: txid: %v\n", t.id)
	if t.getHandler != nil {
		t.getHandler(t.m_tx.m_rootVertex.allReadSet)
	}
	if t.setHandler != nil {
		t.setHandler(t.m_tx.m_rootVertex.allWriteSet, "value")
	}
	// Execute cost - placeholder
	loomExec(t.m_tx.m_rootVertex.m_cost)
}

func (t *OptMETransaction) SetSequence(seq uint64) { t.sequence = seq }
func (t *OptMETransaction) GetSequence() uint64    { return t.sequence }

// Handlers install functions (similar to C++)
func (t *OptMETransaction) InstallGetStorageHandler(h func(readSet map[string]struct{})) {
	t.getHandler = h
}
func (t *OptMETransaction) InstallSetStorageHandler(h func(writeSet map[string]struct{}, value string)) {
	t.setHandler = h
}

func NewOptMETransactionFromInner(inner Transaction, id, blockID uint64) *OptMETransaction {
	// assumes Transaction can be embedded/converted
	return &OptMETransaction{
		Transaction: inner,
		id:          id,
		blockID:     blockID,
		sequence:    0,
		localGetMap: make(map[string]string),
		localPutMap: make(map[string]string),
	}
}

// OptMEEntry and OptMETable

type OptMEEntry struct {
	Value           string
	BlockIDGet      uint64
	ReservedGetTxs  map[*OptMETransaction]struct{}
	BlockIDPut      uint64
	ReservedPutNum  int64
	NextReservedPut int64
}

// Table placeholder interface (you should replace with your real Table implementation)
type TableInterface interface {
	Put(key string, fn func(entry *OptMEEntry))
	Get(key string, fn func(entry *OptMEEntry))
}

type OptMETable struct {
	table TableInterface
}

func NewOptMETable(table TableInterface) *OptMETable {
	return &OptMETable{table: table}
}

func (tbl *OptMETable) ReserveGet(tx *OptMETransaction, k string) {
	tbl.table.Put(k, func(entry *OptMEEntry) {
		// don't have war conflict
		if entry.BlockIDPut == 0 || entry.BlockIDPut == tx.blockID {
			if tx.blockID > entry.BlockIDGet {
				entry.BlockIDGet = tx.blockID
			}
		} else {
			atomic.StoreUint32(&tx.abortedFlag, 1)
			log.Printf("%d:%d reserve get %s failed: put id = %d\n", tx.blockID, tx.id, k, entry.BlockIDPut)
		}
	})
}

func (tbl *OptMETable) ReservePut(tx *OptMETransaction, k string) {
	tbl.table.Put(k, func(entry *OptMEEntry) {
		if entry.BlockIDPut == 0 {
			entry.BlockIDPut = tx.blockID
			entry.ReservedPutNum = 1
		} else if entry.BlockIDPut == tx.blockID {
			entry.ReservedPutNum++
		} else if entry.BlockIDPut < tx.blockID {
			entry.NextReservedPut++
		}
	})
}

// ----------------------
// OptME (protocol master class)
// ----------------------

type OptME struct {
	statistics     *Statistics
	blocks         []*Block
	batches        [][]*OptMETransaction
	acgs           []*AddressBasedConflictGraph
	numThreads     int
	table          *OptMETable
	enableParallel bool
	committedBlock uint64
	pool           ThreadPool
	blockIdx       int
	mtx            sync.Mutex
	cv             *sync.Cond
}

func NewOptME(blocks []*Block, statistics *Statistics, numThreads int, table TableInterface, enableParallel bool, pool ThreadPool) *OptME {
	opt := &OptME{
		statistics:     statistics,
		blocks:         blocks,
		batches:        nil,
		acgs:           nil,
		numThreads:     numThreads,
		table:          NewOptMETable(table),
		enableParallel: enableParallel,
		pool:           pool,
		blockIdx:       0,
	}
	opt.cv = sync.NewCond(&opt.mtx)
	log.Printf("OptME(num_threads=%d, enable_parallel=%v)\n", numThreads, enableParallel)
	return opt
}

func (o *OptME) Start() {
	log.Println("OptME started")
	// split blocks into batches
	for i, block := range o.blocks {
		// block.GetTxs() assumed to return a list of something convertible to Transaction
		txWrappers := GetTxsFromBlock(block) // TODO: replace with your real method
		batch := make([]*OptMETransaction, 0, len(txWrappers))
		batchID := uint64(i + 1)
		for _, txw := range txWrappers {
			txid := txw.GetHyperId() // TODO: replace with real accessor
			oqtx := NewOptMETransactionFromInner(txw.ToTransaction(), uint64(txid), batchID)
			batch = append(batch, oqtx)
		}
		o.acgs = append(o.acgs, NewAddressBasedConflictGraphWithBatch(o.pool, batch))
		o.batches = append(o.batches, batch)
	}
	o.Run()
}

func (o *OptME) Run() {
	for i := 0; i < len(o.batches); i++ {
		batch := o.batches[i]
		acg := o.acgs[i]
		var schedules [][]*OptMETransaction
		var abortedTxs []*OptMETransaction

		o.Simulate(batch)
		if o.enableParallel {
			o.ReorderWithACG(acg, &abortedTxs)
		} else {
			o.Reorder(batch, &abortedTxs)
		}
		o.ParallelExecute(&schedules, &abortedTxs)
		// statistics.JournalBlock() -- placeholder
		log.Printf("Block %d finalize done\n", o.blockIdx)
	}
}

func (o *OptME) Stop() {
	o.pool.Shutdown()
	log.Println("OptME stopped")
}

func (o *OptME) Simulate(batch []*OptMETransaction) {
	o.blockIdx++
	log.Printf("Simulate block %d\n", o.blockIdx)
	var wg sync.WaitGroup
	for _, tx := range batch {
		wg.Add(1)
		txLocal := tx
		doneCh := o.pool.Enqueue(func() {
			defer wg.Done()
			// get handler
			txLocal.InstallGetStorageHandler(func(readSet map[string]struct{}) {
				for k := range readSet {
					o.table.ReserveGet(txLocal, k)
					txLocal.localGetMap[k] = "" // value placeholder
				}
			})
			// set handler
			txLocal.InstallSetStorageHandler(func(writeSet map[string]struct{}, value string) {
				for k := range writeSet {
					o.table.ReservePut(txLocal, k)
					txLocal.localPutMap[k] = value
				}
			})
			txLocal.startTime = time.Now()
			txLocal.Execute()
			// statistics.JournalExecute(); statistics.JournalOverheads(tx->CountOverheads())
			_ = doneCh // ignore
		})
		_ = doneCh // ignore
	}
	wg.Wait()
	log.Printf("Simulate block %d done\n", o.blockIdx)
}

// Reorder (non-ACG)
func (o *OptME) Reorder(simulationResult []*OptMETransaction, abortedTxs *[]*OptMETransaction) {
	log.Printf("Reorder block %d\n", o.blockIdx)
	begin := time.Now()

	var txList []*OptMETransaction
	o.IntraEpochReordering(simulationResult, abortedTxs, &txList)

	// concurrent commit simulation
	for _, tx := range txList {
		latency := time.Since(tx.startTime).Microseconds()
		_ = latency
		// statistics.JournalCommit(LATENCY)
	}

	// statistics.JournalRollbackExecution(...)
	log.Printf("Reorder block %d done\n", o.blockIdx)
	_ = begin
}

// Reorder with ACG
func (o *OptME) ReorderWithACG(acg *AddressBasedConflictGraph, abortedTxs *[]*OptMETransaction) {
	log.Printf("Reorder block %d\n", o.blockIdx)
	begin := time.Now()

	var txList []*OptMETransaction
	o.IntraEpochReorderingWithACG(acg, abortedTxs, &txList)

	for _, tx := range txList {
		latency := time.Since(tx.startTime).Microseconds()
		_ = latency
		// statistics.JournalCommit(LATENCY)
	}

	// statistics.JournalRollbackExecution(...)
	log.Printf("Reorder block %d done\n", o.blockIdx)
	_ = begin
}

// IntraEpochReordering constructs ACG then sorts/reorders
func (o *OptME) IntraEpochReordering(simulationResult []*OptMETransaction, abortedTxs *[]*OptMETransaction, txList *[]*OptMETransaction) {
	acg := NewAddressBasedConflictGraph(o.pool)
	start := time.Now()

	acg.ParallelConstruct(simulationResult)
	constructTime := time.Since(start)
	log.Printf("Construct ACG time: %v ms\n", constructTime.Milliseconds())

	acg.HierarchicalSort()
	sortTime := time.Since(start) - constructTime
	log.Printf("Sort ACG time: %v ms\n", sortTime.Milliseconds())

	acg.Reorder()
	reorderTime := time.Since(start) - constructTime - sortTime
	log.Printf("Reorder ACG time: %v ms\n", reorderTime.Milliseconds())

	*abortedTxs = acg.ExtractAbortedTxs()
	*txList = acg.ExtractTxList()
}

// IntraEpochReordering with ACG passed in
func (o *OptME) IntraEpochReorderingWithACG(acg *AddressBasedConflictGraph, abortedTxs *[]*OptMETransaction, txList *[]*OptMETransaction) {
	start := time.Now()
	acg.HierarchicalSort()
	sortTime := time.Since(start)
	log.Printf("Sort time: %v ms\n", sortTime.Milliseconds())

	acg.Reorder()
	reorderTime := time.Since(start) - sortTime
	log.Printf("Reorder time: %v ms\n", reorderTime.Milliseconds())

	*abortedTxs = acg.ExtractAbortedTxs()
	*txList = acg.ExtractTxList()
}

// InterEpochReordering: reschedule aborted txs into epochs based on conflict
func (o *OptME) InterEpochReordering(schedules *[][]*OptMETransaction, abortedTxs []*OptMETransaction) {
	epochMap := make([]map[string]struct{}, 0)
	for _, tx := range abortedTxs {
		epoch := 0
		for epoch < len(epochMap) && (hasContain(tx.localGetMap, epochMap[epoch]) || hasContain(tx.localPutMap, epochMap[epoch])) {
			epoch++
		}
		if epoch >= len(epochMap) {
			epochMap = append(epochMap, make(map[string]struct{}))
			*schedules = append(*schedules, nil)
		}
		(*schedules)[epoch] = append((*schedules)[epoch], tx)
		for k := range tx.localPutMap {
			epochMap[epoch][k] = struct{}{}
		}
	}
}

// ParallelExecute: re-execute according to schedules
func (o *OptME) ParallelExecute(schedules *[][]*OptMETransaction, abortedTxs *[]*OptMETransaction) {
	log.Printf("ReExecute block %d\n", o.blockIdx)
	begin := time.Now()

	var schedulesLocal [][]*OptMETransaction
	o.InterEpochReordering(&schedulesLocal, *abortedTxs)

	for _, schedule := range schedulesLocal {
		var wg sync.WaitGroup
		for _, tx := range schedule {
			wg.Add(1)
			txLocal := tx
			o.pool.Enqueue(func() {
				defer wg.Done()
				o.ReExecute(txLocal)
				o.Finalize(txLocal)
			})
			// statistics.JournalExecute(); statistics.JournalCommit(LATENCY); statistics.JournalRollback(tx->CountOverheads());
		}
		wg.Wait()
	}

	// statistics.JournalReExecution(...)
	log.Printf("ReExecute block %d done\n", o.blockIdx)
	_ = begin
}

func (o *OptME) Finalize(tx *OptMETransaction) {
	log.Printf("Finalize tx %d\n", tx.id)
	atomic.StoreUint32(&tx.committedFlag, 1)
	for k := range tx.localPutMap {
		kk := k
		o.table.table.Put(kk, func(entry *OptMEEntry) {
			if entry.BlockIDPut == tx.blockID {
				entry.ReservedPutNum--
				if entry.ReservedPutNum == 0 {
					if entry.NextReservedPut > 0 {
						entry.BlockIDPut++
						entry.ReservedPutNum = entry.NextReservedPut
						entry.NextReservedPut = 0
					} else {
						entry.BlockIDPut = 0
					}
				} else if entry.ReservedPutNum < 0 {
					log.Printf("%s reserved put num = %d\n", kk, entry.ReservedPutNum)
				}
			}
		})
	}
}

func (o *OptME) ReExecute(tx *OptMETransaction) {
	// read from public table
	tx.InstallGetStorageHandler(func(readSet map[string]struct{}) {
		for k := range readSet {
			var val string
			k2 := k
			o.table.table.Get(k2, func(entry *OptMEEntry) {
				val = entry.Value
			})
			tx.localGetMap[k] = val
		}
	})
	// write directly
	tx.InstallSetStorageHandler(func(writeSet map[string]struct{}, value string) {
		for k := range writeSet {
			k2 := k
			o.table.table.Put(k2, func(entry *OptMEEntry) {
				entry.Value = value
			})
		}
	})
	tx.Execute()
}

// ----------------------
// Helpers & placeholders
// ----------------------

// hasContain: check if any key in m intersects with set
func hasContain(m map[string]string, set map[string]struct{}) bool {
	for k := range m {
		if _, ok := set[k]; ok {
			return true
		}
	}
	return false
}

// loomExec placeholder: execute "cost"
func loomExec(cost int) {
	// placeholder for the actual workload execution
	_ = cost
}

// GetTxsFromBlock / tx wrapper placeholders
type TxWrapper interface {
	GetHyperId() int64
	ToTransaction() Transaction
}

func GetTxsFromBlock(b *Block) []TxWrapper {
	// TODO: replace with real implementation to extract tx wrappers from block
	return nil
}
