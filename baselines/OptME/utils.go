package OptME

import (
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// ----------------------
// 依赖：请用你项目里的实现替换或保持接口兼容
// ----------------------
// - Transaction, Block, Table, Statistics, Exec 等类型/函数在你代码库已有
// - ThreadPool 是一个简单抽象：Enqueue(fn) -> return done chan struct{}，GetThreadNum()
// ----------------------

// ThreadPool is a small interface abstraction for a goroutine-pool.
// Replace with your project's pool implementation.
type ThreadPool interface {
	GetThreadNum() int
	// Enqueue executes fn in the pool; returns a channel that will be closed when fn returns.
	Enqueue(fn func()) <-chan struct{}
	Shutdown()
}

// Placeholder types - replace with real implementations.
type Transaction struct{}
type Block struct{}
type Statistics struct{}

// ----------------------
// Unit related
// ----------------------

type UnitType int

const (
	UnitTypeRead UnitType = iota
	UnitTypeWrite
)

type Unit struct {
	tx             *OptMETransaction
	unitType       UnitType
	address        string
	lock           sync.RWMutex
	wrDependencies uint32
	coLocated      bool
}

func NewUnit(tx *OptMETransaction, utype UnitType, addr string, coLocated bool) *Unit {
	return &Unit{
		tx:             tx,
		unitType:       utype,
		address:        addr,
		coLocated:      coLocated,
		wrDependencies: 0,
	}
}

func (u *Unit) GetUnitType() UnitType { return u.unitType }
func (u *Unit) GetAddress() string {
	u.lock.RLock()
	defer u.lock.RUnlock()
	return u.address
}
func (u *Unit) GetDegree() uint32 {
	u.lock.RLock()
	defer u.lock.RUnlock()
	return u.wrDependencies
}
func (u *Unit) AddDependency() {
	u.lock.Lock()
	u.wrDependencies++
	u.lock.Unlock()
}
func (u *Unit) IsSorted() bool {
	// sequence > 0 means sorted in original C++ code
	return u.tx != nil && u.tx.GetSequence() > 0
}
func (u *Unit) AbortTx() {
	if u.tx == nil {
		return
	}
	atomic.StoreUint32(&u.tx.abortedFlag, 1)
}

// ----------------------
// ReadUnits & WriteUnits
// ----------------------

type ReadUnits struct {
	Units  []*Unit
	MaxSeq uint32
}

func NewReadUnits(units []*Unit) *ReadUnits {
	return &ReadUnits{Units: units, MaxSeq: 0}
}

func (r *ReadUnits) Push(u *Unit) {
	r.Units = append(r.Units, u)
}

func (r *ReadUnits) Sort() {
	var sorted []*Unit
	var remaining []*Unit
	for _, u := range r.Units {
		if u.IsSorted() {
			sorted = append(sorted, u)
		} else {
			remaining = append(remaining, u)
		}
	}

	if len(sorted) == 0 {
		r.MaxSeq = 1
	} else {
		// find min and max sequence among sorted
		minSeq := sorted[0].tx.GetSequence()
		maxSeq := sorted[0].tx.GetSequence()
		for _, u := range sorted[1:] {
			seq := u.tx.GetSequence()
			if seq < minSeq {
				minSeq = seq
			}
			if seq > maxSeq {
				maxSeq = seq
			}
		}
		r.MaxSeq = maxSeq
		_ = minSeq // minSeq not used but mirrored from C++ logic
	}

	// set sequence for remaining units to max_seq
	for _, u := range remaining {
		u.tx.SetSequence(uint64(r.MaxSeq))
	}

	// move sorted + remaining back to Units
	r.Units = append(sorted, remaining...)
}

type WriteUnits struct {
	Units            []*Unit
	MaxSeq           uint32
	firstUpdaterFlag bool
}

func NewWriteUnits(units []*Unit) *WriteUnits {
	return &WriteUnits{Units: units, MaxSeq: 0, firstUpdaterFlag: false}
}

func (w *WriteUnits) Push(u *Unit) {
	w.Units = append(w.Units, u)
}

// Sort takes ReadUnits to coordinate sequences
func (w *WriteUnits) Sort(r *ReadUnits) {
	var sorted []*Unit
	var remaining []*Unit
	for _, u := range w.Units {
		if u.IsSorted() {
			sorted = append(sorted, u)
		} else {
			remaining = append(remaining, u)
		}
	}

	// for sorted, if not aborted and co_located -> first updater gets seq read.max+1, others aborted
	for _, u := range sorted {
		if u.tx != nil && atomic.LoadUint32(&u.tx.abortedFlag) == 0 && u.coLocated {
			if !w.firstUpdaterFlag {
				u.tx.SetSequence(uint64(r.MaxSeq + 1))
				w.firstUpdaterFlag = true
			} else {
				log.Printf("abort tx by unit: %d\n", u.tx.id)
				u.AbortTx()
			}
		}
	}

	// abort sorted ones with sequence < read.MaxSeq
	for _, u := range sorted {
		if u.tx != nil && atomic.LoadUint32(&u.tx.abortedFlag) == 0 && u.tx.GetSequence() < uint64(r.MaxSeq) {
			log.Printf("abort tx by unit: %d\n", u.tx.id)
			u.AbortTx()
		}
	}

	// collect used write sequences
	writeSeqSet := make(map[uint32]struct{})
	for _, u := range sorted {
		if u.tx != nil {
			writeSeqSet[uint32(u.tx.GetSequence())] = struct{}{}
		}
	}

	writeSeq := r.MaxSeq + 1
	for _, u := range remaining {
		for {
			if _, exists := writeSeqSet[writeSeq]; !exists {
				break
			}
			writeSeq++
		}
		u.tx.SetSequence(uint64(writeSeq))
		writeSeqSet[writeSeq] = struct{}{}
	}

	w.MaxSeq = writeSeq
	// reorder units: sorted then remaining
	w.Units = append(sorted, remaining...)
}

// ----------------------
// Address
// ----------------------

type Address struct {
	AddressStr       string
	InDegree         uint32
	OutDegree        uint32
	ReadUnits        *ReadUnits
	WriteUnits       *WriteUnits
	FirstUpdaterFlag bool
}

func NewAddress(addr string) *Address {
	return &Address{
		AddressStr: addr,
		ReadUnits:  NewReadUnits(nil),
		WriteUnits: NewWriteUnits(nil),
	}
}

func (a *Address) AddUnit(u *Unit) {
	if u.GetUnitType() == UnitTypeRead {
		a.InDegree += u.GetDegree()
		a.ReadUnits.Push(u)
	} else {
		if u.coLocated {
			a.FirstUpdaterFlag = true
		}
		a.OutDegree += u.GetDegree()
		a.WriteUnits.Push(u)
	}
}

func (a *Address) SortReadUnits() {
	a.ReadUnits.Sort()
}
func (a *Address) SortWriteUnits() {
	a.WriteUnits.Sort(a.ReadUnits)
}

func (a *Address) Merge(other *Address) {
	// if both have first_updater_flag, need to abort some tx in other (C++ logic)
	if a.FirstUpdaterFlag && other.FirstUpdaterFlag {
		for _, unit := range other.ReadUnits.Units {
			if unit.coLocated {
				log.Printf("abort tx by unit: %d\n", unit.tx.id)
				unit.AbortTx()
				a.InDegree += other.InDegree - unit.GetDegree()
				break
			}
		}
		for _, unit := range other.WriteUnits.Units {
			if unit.coLocated {
				log.Printf("abort tx by unit: %d\n", unit.tx.id)
				unit.AbortTx()
				a.InDegree += other.InDegree - unit.GetDegree()
				break
			}
		}
	} else {
		a.InDegree += other.InDegree
		a.OutDegree += other.OutDegree
		a.ReadUnits.Units = append(a.ReadUnits.Units, other.ReadUnits.Units...)
		a.WriteUnits.Units = append(a.WriteUnits.Units, other.WriteUnits.Units...)
		a.FirstUpdaterFlag = a.FirstUpdaterFlag || other.FirstUpdaterFlag
	}
}

// ----------------------
// AddressBasedConflictGraph
// ----------------------

type AddressBasedConflictGraph struct {
	Addresses  map[string]*Address
	TxList     []*OptMETransaction
	AbortedTxs []*OptMETransaction
	Pool       ThreadPool
}

func NewAddressBasedConflictGraph(pool ThreadPool) *AddressBasedConflictGraph {
	return &AddressBasedConflictGraph{
		Addresses: make(map[string]*Address),
		Pool:      pool,
	}
}

func NewAddressBasedConflictGraphWithBatch(pool ThreadPool, batch []*OptMETransaction) *AddressBasedConflictGraph {
	acg := NewAddressBasedConflictGraph(pool)
	acg.Initialize(batch)
	return acg
}

// Construct from simulation result (non-parallel version)
func (acg *AddressBasedConflictGraph) Construct(simulationResult []*OptMETransaction) {
	for _, tx := range simulationResult {
		writeUnits := NewWriteUnits(acg.convertToUnits(tx, UnitTypeWrite, tx.localPutMap, tx.localGetMap))

		if acg.checkUpdaterAlreadyExistInSameAddress(writeUnits.Units) {
			atomic.StoreUint32(&tx.abortedFlag, 1)
			acg.AbortedTxs = append(acg.AbortedTxs, tx)
			continue
		}

		readUnits := NewReadUnits(acg.convertToUnits(tx, UnitTypeRead, tx.localGetMap, nil))
		acg.setWrDependencies(readUnits.Units, writeUnits.Units)

		acg.TxList = append(acg.TxList, tx)
		acg.addUnitsToAddress(readUnits.Units)
		acg.addUnitsToAddress(writeUnits.Units)
	}
}

// Parallel construct: splits into chunks, enqueue subgraphs, then merge
func (acg *AddressBasedConflictGraph) ParallelConstruct(simulationResult []*OptMETransaction) {
	numOfTxn := len(simulationResult)
	ncpu := acg.Pool.GetThreadNum()
	chunkSize := numOfTxn / ncpu
	if chunkSize == 0 {
		chunkSize = 1
	}

	type futureResult struct {
		sub  *AddressBasedConflictGraph
		done <-chan struct{}
	}

	var futures []<-chan *AddressBasedConflictGraph
	// We implement Enqueue returning a done channel; but we need also to retrieve result.
	// For simplicity, we spawn goroutines here using pool to run construction and return subgraphs via channels.
	resultChans := make([]chan *AddressBasedConflictGraph, 0)

	for i := 0; i < numOfTxn; i += chunkSize {
		end := i + chunkSize
		if end > numOfTxn {
			end = numOfTxn
		}
		ch := make(chan *AddressBasedConflictGraph, 1)
		resultChans = append(resultChans, ch)

		// Capture slice
		i0, end0 := i, end
		acg.Pool.Enqueue(func() {
			chunk := make([]*OptMETransaction, 0, end0-i0)
			for k := i0; k < end0; k++ {
				chunk = append(chunk, simulationResult[k])
			}
			subGraph := NewAddressBasedConflictGraph(acg.Pool)
			subGraph.Construct(chunk)
			ch <- subGraph
			close(ch)
		})
	}

	subGraphs := make([]*AddressBasedConflictGraph, 0, len(resultChans))
	for _, ch := range resultChans {
		sub := <-ch
		subGraphs = append(subGraphs, sub)
	}

	// reduce/merge subGraphs in-place in pairs (simple serial reduction to avoid complex parallel merges)
	for len(subGraphs) > 1 {
		var merged []*AddressBasedConflictGraph
		for i := 0; i < len(subGraphs); i += 2 {
			if i+1 < len(subGraphs) {
				subGraphs[i].Merge(subGraphs[i+1])
			}
			merged = append(merged, subGraphs[i])
		}
		subGraphs = merged
	}

	if len(subGraphs) == 1 {
		// move contents
		*this = *subGraphs[0]
	}
}

// Initialize used in OptME initialization path
func (acg *AddressBasedConflictGraph) Initialize(batch []*OptMETransaction) {
	for _, tx := range batch {
		// convertToUnits2 in C++ used unordered_set; here we reuse convertToUnits with maps
		writeUnits := NewWriteUnits(acg.convertToUnits2(tx, UnitTypeWrite, tx.m_tx.m_rootVertex.allWriteSet, tx.m_tx.m_rootVertex.allReadSet))
		if acg.checkUpdaterAlreadyExistInSameAddress(writeUnits.Units) {
			atomic.StoreUint32(&tx.abortedFlag, 1)
			acg.AbortedTxs = append(acg.AbortedTxs, tx)
			continue
		}
		readUnits := NewReadUnits(acg.convertToUnits2(tx, UnitTypeRead, tx.m_tx.m_rootVertex.allReadSet, nil))
		acg.setWrDependencies(readUnits.Units, writeUnits.Units)

		acg.TxList = append(acg.TxList, tx)
		acg.addUnitsToAddress(readUnits.Units)
		acg.addUnitsToAddress(writeUnits.Units)
	}
}

// convertToUnits: map version
func (acg *AddressBasedConflictGraph) convertToUnits(tx *OptMETransaction, unitType UnitType, readOrWrite map[string]string, readSet map[string]string) []*Unit {
	units := make([]*Unit, 0, len(readOrWrite))
	for key := range readOrWrite {
		coLocate := false
		if readSet != nil {
			if _, ok := readSet[key]; ok {
				coLocate = true
			}
		}
		units = append(units, NewUnit(tx, unitType, key, coLocate))
	}
	return units
}

// convertToUnits2: set version
func (acg *AddressBasedConflictGraph) convertToUnits2(tx *OptMETransaction, unitType UnitType, readOrWrite map[string]struct{}, readSet map[string]struct{}) []*Unit {
	units := make([]*Unit, 0, len(readOrWrite))
	for key := range readOrWrite {
		coLocate := false
		if readSet != nil {
			if _, ok := readSet[key]; ok {
				coLocate = true
			}
		}
		units = append(units, NewUnit(tx, unitType, key, coLocate))
	}
	return units
}

func (acg *AddressBasedConflictGraph) checkUpdaterAlreadyExistInSameAddress(writeUnits []*Unit) bool {
	for _, u := range writeUnits {
		if u.coLocated {
			if addr, ok := acg.Addresses[u.GetAddress()]; ok {
				if addr.FirstUpdaterFlag {
					return true
				}
			}
		}
	}
	return false
}

func (acg *AddressBasedConflictGraph) setWrDependencies(readUnits, writeUnits []*Unit) {
	for _, w := range writeUnits {
		addr := w.GetAddress()
		for _, r := range readUnits {
			if r.GetAddress() != addr {
				r.AddDependency()
				w.AddDependency()
			}
		}
	}
}

func (acg *AddressBasedConflictGraph) addUnitsToAddress(units []*Unit) {
	for _, u := range units {
		raw := u.GetAddress()
		if _, ok := acg.Addresses[raw]; !ok {
			acg.Addresses[raw] = NewAddress(raw)
		}
		acg.Addresses[raw].AddUnit(u)
	}
}

func (acg *AddressBasedConflictGraph) HierarchicalSort() {
	for _, key := range acg.AddressRank() {
		addr := acg.Addresses[key]
		addr.SortReadUnits()
		addr.SortWriteUnits()
	}
}

func (acg *AddressBasedConflictGraph) AddressRank() []string {
	addrList := make([]*Address, 0, len(acg.Addresses))
	for _, a := range acg.Addresses {
		addrList = append(addrList, a)
	}
	sort.Slice(addrList, func(i, j int) bool {
		a := addrList[i]
		b := addrList[j]
		if a.InDegree != b.InDegree {
			return a.InDegree > b.InDegree
		}
		if a.OutDegree != b.OutDegree {
			return a.OutDegree < b.OutDegree
		}
		return a.AddressStr < b.AddressStr
	})
	ranked := make([]string, 0, len(addrList))
	for _, a := range addrList {
		ranked = append(ranked, a.AddressStr)
	}
	return ranked
}

func (acg *AddressBasedConflictGraph) Reorder() {
	allAborted := acg.extractAbortList()
	var reorderTargets []*OptMETransaction
	var aborted []*OptMETransaction

	for _, tx := range allAborted {
		if len(tx.localGetMap) == 0 && len(tx.localPutMap) > 1 {
			reorderTargets = append(reorderTargets, tx)
		} else {
			aborted = append(aborted, tx)
		}
	}

	acg.AbortedTxs = append(acg.AbortedTxs, aborted...)
	// sort aborted_txs by id
	sort.Slice(acg.AbortedTxs, func(i, j int) bool {
		return acg.AbortedTxs[i].id < acg.AbortedTxs[j].id
	})

	for _, tx := range reorderTargets {
		var seq uint32 = 0
		uniqueAddresses := make(map[string]struct{})
		for k := range tx.localPutMap {
			uniqueAddresses[k] = struct{}{}
		}
		for addr := range uniqueAddresses {
			if a, ok := acg.Addresses[addr]; ok {
				if a.WriteUnits.MaxSeq > a.ReadUnits.MaxSeq {
					if uint32(a.WriteUnits.MaxSeq) > seq {
						seq = uint32(a.WriteUnits.MaxSeq)
					}
				} else {
					if a.ReadUnits.MaxSeq > seq {
						seq = a.ReadUnits.MaxSeq
					}
				}
			}
		}
		tx.SetSequence(uint64(seq))
		acg.TxList = append(acg.TxList, tx)
	}
}

func (acg *AddressBasedConflictGraph) extractAbortList() []*OptMETransaction {
	abortedList := make([]*OptMETransaction, 0)
	remaining := make([]*OptMETransaction, 0)

	for _, tx := range acg.TxList {
		if atomic.LoadUint32(&tx.abortedFlag) != 0 {
			abortedList = append(abortedList, tx)
		} else {
			remaining = append(remaining, tx)
		}
	}
	acg.TxList = remaining
	return abortedList
}

func (acg *AddressBasedConflictGraph) Merge(other *AddressBasedConflictGraph) {
	for addr, address := range other.Addresses {
		if existing, ok := acg.Addresses[addr]; ok {
			existing.Merge(address)
		} else {
			acg.Addresses[addr] = address
		}
	}
	acg.TxList = append(acg.TxList, other.TxList...)
	acg.AbortedTxs = append(acg.AbortedTxs, other.AbortedTxs...)
}

func (acg *AddressBasedConflictGraph) ExtractAbortedTxs() []*OptMETransaction { return acg.AbortedTxs }
func (acg *AddressBasedConflictGraph) ExtractTxList() []*OptMETransaction     { return acg.TxList }

// Utility: join keys for logging
func joinKeys(m map[string]string) string {
	parts := make([]string, 0, len(m))
	for k := range m {
		parts = append(parts, k)
	}
	return strings.Join(parts, " ")
}
