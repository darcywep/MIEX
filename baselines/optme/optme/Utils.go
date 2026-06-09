package optme

import (
	"Janus/baselines/common"
	"Janus/tools"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

type UnitType int

const (
	Read UnitType = iota
	Write
)

// Unit 事务处理，Unit 表示一个事务操作单元，用于并发控制和依赖管理(基于哪吒，先读后写，r(出边)指向w(入边))
type Unit struct {
	tx             *OptmeTransaction
	unitType       UnitType
	address        string
	lock           sync.RWMutex
	wrDependencies uint32
	co_located     atomic.Bool // 是否读写均有
}

func newUnit(tx *OptmeTransaction, unitType UnitType, address string, co_located atomic.Bool) *Unit {
	return &Unit{
		tx:             tx,
		unitType:       unitType,
		address:        address,
		co_located:     co_located,
		wrDependencies: 0,
	}
}

func (u *Unit) getDegree() uint32 {
	u.lock.RLock()
	defer u.lock.RUnlock()
	return u.wrDependencies
}

func (u *Unit) addDependence() {
	u.lock.RLock()
	defer u.lock.RUnlock()
	u.wrDependencies += 1
}

func (u *Unit) is_sorted() bool {
	return u.tx.Sequenceid > 0
}

func (u *Unit) abort_tx(txid string) {
	u.tx.Aborted.Store(true)
}

// ReadUnits 管理读操作单元的集合，支持排序和序列号分配
type ReadUnits struct {
	units   []*Unit
	max_seq uint32
}

func NewReadUnits(units []*Unit) *ReadUnits {
	return &ReadUnits{units, 0}
}

func (u *ReadUnits) push(unit *Unit) {
	u.units = append(u.units, unit)
}

func (u *ReadUnits) sort() {
	var sorted, remaining []*Unit
	// 分区：已排序和未排序
	for _, unit := range u.units {
		if unit.is_sorted() {
			sorted = append(sorted, unit)
		} else {
			remaining = append(remaining, unit)
		}
	}

	if len(sorted) == 0 {
		u.max_seq = 1
	} else {
		// 找到已排序单元中的最大序列号
		maxSeq := uint32(0)
		for _, unit := range sorted {
			seq := unit.tx.Sequenceid
			if seq > maxSeq {
				maxSeq = seq
			}
		}
		u.max_seq = maxSeq

		//// 对已排序单元按序列号排序
		//sort.Slice(sorted, func(i, j int) bool {
		//	return sorted[i].tx.sequenceid < sorted[j].tx.sequenceid
		//})
	}

	// 为未排序单元设置序列号
	for _, unit := range remaining {
		unit.tx.Sequenceid = u.max_seq
	}

	// 合并结果（已排序的在前，新设置的在后）
	u.units = make([]*Unit, 0, len(sorted)+len(remaining))
	u.units = append(u.units, sorted...)
	u.units = append(u.units, remaining...)
}

// WriteUnits 管理写操作单元的集合，处理写冲突和序列号分配
type WriteUnits struct {
	units             []*Unit
	max_seq           uint32
	first_update_flag bool // 冲突标记，是否已存在一个写操作（co_locate主要用作）
}

func NewWriteUnits(units []*Unit) *WriteUnits {
	return &WriteUnits{units, 0, false}
}

func (u *WriteUnits) push(unit *Unit) {
	u.units = append(u.units, unit)
}

func (u *WriteUnits) sort(readunits *ReadUnits) {
	var sorted, remaining []*Unit
	// 分区：已排序和未排序的单元
	for _, unit := range u.units {
		if unit.is_sorted() {
			sorted = append(sorted, unit)
		} else {
			remaining = append(remaining, unit)
		}
	}

	// 处理已排序的单元
	for _, unit := range sorted {
		tx := unit.tx

		if !tx.Aborted.Load() && unit.co_located.Load() { // 写集也在读集中
			if !u.first_update_flag { // 第一次冲突
				tx.Sequenceid = readunits.max_seq + 1
				u.first_update_flag = true
				if tools.TraceOptMELog {
					fmt.Printf("[OptME debug] set sequence for first updater: %d\n", readunits.max_seq)
				}
			} else {
				//fmt.Printf("DEBUG: abort tx by unit: %v", unit.tx.Tx)
				unit.tx.Aborted.Store(true) // 剩下交易全部abort
			}
		}
	}

	// 再次检查已排序单元（有交易的sequenceid可能比readunits.max_seq小，来自上一个epoch）
	for _, unit := range sorted {
		tx := unit.tx
		// 修正：使用 GetAborted() 返回的 bool 值
		if !tx.Aborted.Load() && tx.Sequenceid < readunits.max_seq {
			//log.Printf("DEBUG: abort tx by unit: %d", tx.Tx.Txid)
			unit.tx.Aborted.Store(true)
		}
	}

	// 收集所有写入序列号（只收集未中止的事务）
	writeSeqSet := make(map[uint32]bool)
	for _, unit := range sorted {
		tx := unit.tx
		if !tx.Aborted.Load() { // 使用 GetAborted() 返回的 bool 值
			writeSeqSet[tx.Sequenceid] = true
		}
	}

	// 为未排序单元分配序列号
	writeSeq := readunits.max_seq + 1
	for _, unit := range remaining {
		// 找到可用的序列号
		for writeSeqSet[writeSeq] {
			writeSeq++
		}
		unit.tx.Sequenceid = writeSeq
		writeSeqSet[writeSeq] = true
	}

	u.max_seq = writeSeq

	// 重新组合单元
	u.units = make([]*Unit, 0, len(sorted)+len(remaining))
	u.units = append(u.units, sorted...)
	u.units = append(u.units, remaining...)
}

type Address struct {
	address            string
	in_degree          uint32
	out_degree         uint32
	readUnits          ReadUnits
	writeUnits         WriteUnits
	first_updater_flag bool
} // Address 表示一批对同一个Key的读和写操作集合，多线程并行构图需要将多个address合并

func NewAddress(address string) *Address {
	return &Address{address: address}
}

func (a *Address) AddUnit(u *Unit) {
	if u.unitType == Read {
		a.in_degree += u.getDegree()
		a.readUnits.push(u)
	} else {
		if u.co_located.Load() == true {
			a.first_updater_flag = true
		}
		a.out_degree += u.getDegree()
		a.writeUnits.push(u)
	}
}

func (a *Address) sort_read_unit() {
	a.readUnits.sort()
}

func (a *Address) sort_write_unit() {
	a.writeUnits.sort(&a.readUnits)
}

func (a *Address) Merge(other *Address) {
	if a.first_updater_flag && other.first_updater_flag { // 两个子图中都有first_updater_flag
		for _, unit := range other.readUnits.units { // 遍历右边子图的读操作
			if unit.co_located.Load() == true { // 找到右边子图的first committer
				unit.tx.Aborted.Store(true) // 中止交易
				a.in_degree += other.in_degree - unit.getDegree()
				break
			}
		}

		for _, unit := range other.writeUnits.units {
			if unit.co_located.Load() == true {
				unit.tx.Aborted.Store(true) // 中止交易
				a.out_degree += other.out_degree - unit.getDegree()
				break
			}
		}
	} else {
		a.in_degree += other.in_degree
		a.out_degree += other.out_degree
		a.readUnits.units = append(a.readUnits.units, other.readUnits.units...)
		a.writeUnits.units = append(a.writeUnits.units, other.writeUnits.units...)
		a.first_updater_flag = a.first_updater_flag || other.first_updater_flag
	}
}

type AddressBasedConflictGraph struct {
	addresses  map[string]*Address // 多个address
	txList     []*OptmeTransaction
	abortedTxs []*OptmeTransaction
	pool       *common.ThreadPool
}

func NewAddressBasedConflictGraph(pool *common.ThreadPool) *AddressBasedConflictGraph {
	return &AddressBasedConflictGraph{
		addresses: make(map[string]*Address),
		pool:      pool,
	}
}

// ConvertToUnits 将交易的读写集合转换为读写单元unit（包含值的版本），readOrWriteSet指的是读集或者写集，不是读写集所有
func (a *AddressBasedConflictGraph) ConvertToUnits(tx *OptmeTransaction, unitType UnitType, readOrWriteSet map[string]string, readSet map[string]string) []*Unit {
	//fmt.Printf("Ready to Run ConvertToUnits... \n")
	//fmt.Printf("readOrWriteSet size: %d \n", len(readOrWriteSet))

	units := make([]*Unit, 0, len(readOrWriteSet))
	for key, _ := range readOrWriteSet { // 遍历写集
		var coLocate atomic.Bool
		coLocate.Store(false)
		if len(readSet) > 0 {
			// 检查读写集合中是否存在相同的键（co-location）
			if _, exists := readSet[key]; exists {
				coLocate.Store(true)
			}
		}
		units = append(units, newUnit(tx, unitType, key, coLocate)) // 使用 key 和 value 创建 Unit
	}
	return units
}

func (a *AddressBasedConflictGraph) ConvertToUnits2(tx *OptmeTransaction, unitType UnitType, readOrWriteSet map[string]string, readSet map[string]string) []*Unit {
	units := make([]*Unit, 0, len(readOrWriteSet))

	for key := range readOrWriteSet {
		var coLocate atomic.Bool
		coLocate.Store(false)
		if len(readSet) > 0 {
			// read/write set co-location
			if _, exists := readSet[key]; exists {
				coLocate.Store(true)
			}
		}
		units = append(units, newUnit(tx, unitType, key, coLocate))
	}
	return units
}

func (a *AddressBasedConflictGraph) CheckUpdaterAlreadyExistInSameAddress(writeUnits []*Unit) bool {
	for _, unit := range writeUnits {
		if unit.co_located.Load() == true {

			address := unit.address
			if existingUnit, exists := a.addresses[address]; exists && existingUnit.first_updater_flag == true {
				return true // 即冲突
			}

		}
	}
	return false
}

// SetWRDependencies 设置读写依赖关系
func (a *AddressBasedConflictGraph) SetWRDependencies(readUnits []*Unit, writeUnits []*Unit) {
	for _, writeUnit := range writeUnits {
		address := writeUnit.address
		for _, readUnit := range readUnits {
			if readUnit.address == address {
				readUnit.addDependence()
				writeUnit.addDependence()
			}
		}
	}
}

// AddUnitsToAddress 将单元添加到地址映射中
func (a *AddressBasedConflictGraph) AddUnitsToAddress(units []*Unit) {
	for _, unit := range units {
		rawAddress := unit.address

		// 如果地址不存在，创建新的地址
		if _, exists := a.addresses[rawAddress]; !exists {
			a.addresses[rawAddress] = NewAddress(rawAddress)
		}

		// 将单元添加到地址中
		a.addresses[rawAddress].AddUnit(unit)
	}
}

func (a *AddressBasedConflictGraph) Initialize(batch []*OptmeTransaction) {
	for _, tx := range batch {

		writeUnits := a.ConvertToUnits2(tx, UnitType(Write), tx.Tx.Vertex.WriteKeys, tx.Tx.Vertex.ReadKeys)

		if a.CheckUpdaterAlreadyExistInSameAddress(writeUnits) {
			tx.Aborted.Store(true)
			a.abortedTxs = append(a.abortedTxs, tx)
			continue
		}

		readUnits := a.ConvertToUnits2(tx, UnitType(Read), tx.Tx.Vertex.ReadKeys, nil)
		a.SetWRDependencies(readUnits, writeUnits)

		a.txList = append(a.txList, tx)
		a.AddUnitsToAddress(readUnits)
		a.AddUnitsToAddress(writeUnits)
	}
}

func (a *AddressBasedConflictGraph) NewAddressBasedConflictGraphWithBatch(pool *common.ThreadPool, batch []*OptmeTransaction) *AddressBasedConflictGraph {
	graph := &AddressBasedConflictGraph{
		pool: pool,
	}
	graph.Initialize(batch)
	return graph
}

// Go语言中通常使用指针赋值或重新分配
func (a *AddressBasedConflictGraph) AssignFrom(other *AddressBasedConflictGraph) {
	if a != other { // 防止深拷贝
		a.addresses = other.addresses // 切片/映射是引用类型，直接赋值
		a.txList = other.txList
		a.abortedTxs = other.abortedTxs
	}
}

// Construct 构建冲突图
// 参数:
//   - simulationResult: 模拟结果，包含所有待处理的事务
func (a *AddressBasedConflictGraph) Construct(simulationResult []*OptmeTransaction) {

	//fmt.Print("Ready to Run Construct ConflictGraph... \n")
	for _, tx := range simulationResult {

		//	转换写单元
		writeUnits := a.ConvertToUnits(tx, UnitType(Write), tx.Tx.Vertex.WriteKeys, tx.Tx.Vertex.ReadKeys)
		//writeUnits := a.ConvertToUnits(tx, UnitType(Write), tx.LocalPut, tx.LocalGet)

		//	检查同一地址是否已存在更新器
		if a.CheckUpdaterAlreadyExistInSameAddress(writeUnits) {
			tx.Aborted.Store(true)
			a.abortedTxs = append(a.abortedTxs, tx)
			//fmt.Printf("跳过已中止的交易...")
			continue // 跳过已中止的事务
		}

		// 转换读单元
		readUnits := a.ConvertToUnits(tx, UnitType(Read), tx.Tx.Vertex.ReadKeys, nil)

		// 设置读写依赖关系
		a.SetWRDependencies(readUnits, writeUnits)

		// 添加到事务列表和地址映射
		a.txList = append(a.txList, tx)
		a.AddUnitsToAddress(readUnits)
		a.AddUnitsToAddress(writeUnits)
	}
}

// ParallelConstruct 并行构建冲突图
// 参数:
//   - simulationResult: 模拟结果，包含所有待处理的事务
//   - threadNum: 显式并发切分数量，来自命令行 -t，而不是机器 CPU 数
func (a *AddressBasedConflictGraph) ParallelConstruct(simulationResult []*OptmeTransaction, threadNum int) {
	numOfTxn := len(simulationResult)
	if numOfTxn == 0 {
		return
	}
	workerNum := threadNum
	if workerNum <= 0 {
		workerNum = 1
	}
	if workerNum > numOfTxn {
		workerNum = numOfTxn
	}
	chunkSize := (numOfTxn + workerNum - 1) / workerNum // 每个线程处理的交易数目

	var wg sync.WaitGroup
	partials := make([]*AddressBasedConflictGraph, workerNum)

	// 第一阶段：并行处理子任务
	for chunkID := 0; chunkID < workerNum; chunkID++ {
		start := chunkID * chunkSize
		end := min(start+chunkSize, numOfTxn)
		if start >= end {
			continue
		}

		wg.Add(1)
		go func(chunkID, start, end int) {
			defer wg.Done()

			// 创建子数据块
			chunk := make([]*OptmeTransaction, end-start)
			copy(chunk, simulationResult[start:end])

			// 构建子冲突图
			subGraph := NewAddressBasedConflictGraph(a.pool)
			subGraph.Construct(chunk)

			partials[chunkID] = subGraph
		}(chunkID, start, end)
	}

	// 等待所有子任务完成
	wg.Wait()

	// 按 chunkID 收集子图，避免 goroutine 完成顺序影响后续 merge 顺序。
	subGraphs := make([]*AddressBasedConflictGraph, 0, workerNum)
	for _, subGraph := range partials {
		if subGraph != nil {
			subGraphs = append(subGraphs, subGraph)
		}
	}

	// 第二阶段：并行归并子图
	for len(subGraphs) > 1 {
		var mergedGraphs []*AddressBasedConflictGraph
		var mergeWg sync.WaitGroup
		var mu sync.Mutex

		// 使用分治法并行合并
		for i := 0; i < len(subGraphs); i += 2 {
			if i+1 >= len(subGraphs) {
				// 单个子图直接加入结果
				mu.Lock()
				mergedGraphs = append(mergedGraphs, subGraphs[i])
				mu.Unlock()
				continue
			}

			mergeWg.Add(1)
			go func(leftIdx, rightIdx int) {
				defer mergeWg.Done()

				// 合并两个子图
				subGraphs[leftIdx].Merge(subGraphs[rightIdx])

				mu.Lock()
				mergedGraphs = append(mergedGraphs, subGraphs[leftIdx])
				mu.Unlock()
			}(i, i+1)
		}

		mergeWg.Wait()
		subGraphs = mergedGraphs
	}

	// 最终结果合并到当前对象
	if len(subGraphs) > 0 {
		a.Merge(subGraphs[0])
	}
}

// Merge 将另一个冲突图合并到当前冲突图中
// 参数:
//   - other: 要合并的另一个冲突图
func (a *AddressBasedConflictGraph) Merge(other *AddressBasedConflictGraph) {
	// 合并地址映射
	for addr, address := range other.addresses {
		if existingAddr, exists := a.addresses[addr]; exists {
			// 如果地址已存在，合并地址内容
			existingAddr.Merge(address)
		} else {
			// 如果地址不存在，直接添加
			a.addresses[addr] = address
		}
	}

	// 合并事务列表
	a.txList = append(a.txList, other.txList...)

	// 合并中止事务列表
	a.abortedTxs = append(a.abortedTxs, other.abortedTxs...)
}

// addressRank 地址排名（确定处理顺序）
func (a *AddressBasedConflictGraph) addressRank() []string {
	// 创建地址列表
	addressList := make([]*Address, 0, len(a.addresses))
	for _, addr := range a.addresses {
		addressList = append(addressList, addr)
	}

	// 排序：入度降序，出度升序，地址升序
	sort.Slice(addressList, func(i, j int) bool {
		addrI := addressList[i]
		addrJ := addressList[j]

		if addrI.in_degree != addrJ.in_degree {
			return addrI.in_degree > addrJ.in_degree
		}
		if addrI.out_degree != addrJ.out_degree {
			return addrI.out_degree < addrJ.out_degree
		}
		return addrI.address < addrJ.address
	})

	// 提取排序后的地址列表
	rankedAddresses := make([]string, len(addressList))
	for i, addr := range addressList {
		rankedAddresses[i] = addr.address
	}

	return rankedAddresses
}

// HierarchicalSort 分层排序
func (a *AddressBasedConflictGraph) HierarchicalSort() {
	// 获取按优先级排序的地址列表
	rankedAddresses := a.addressRank()

	for _, key := range rankedAddresses {
		// 排序读单元和写单元
		address := a.addresses[key]
		address.sort_read_unit()
		address.sort_write_unit()
	}
}

// Reorder 重排序中止的交易
func (a *AddressBasedConflictGraph) Reorder() {
	allAborted := a.extractAbortList()
	var reorderTargets []*OptmeTransaction
	var aborted []*OptmeTransaction

	// 分区复制：满足条件的放入reorderTargets，其他的放入aborted
	for _, tx := range allAborted {
		if len(tx.LocalGet) == 0 && len(tx.LocalPut) > 1 {
			reorderTargets = append(reorderTargets, tx)
		} else {
			aborted = append(aborted, tx)
		}
	}

	// 将aborted交易加入到总的abortedTxs列表中
	a.abortedTxs = append(a.abortedTxs, aborted...)

	// 按交易ID排序abortedTxs
	sort.Slice(a.abortedTxs, func(i, j int) bool {
		return a.abortedTxs[i].Tx.Txid < a.abortedTxs[j].Tx.Txid
	})

	// 处理重排序目标交易
	for _, tx := range reorderTargets {
		var seq uint32
		uniqueAddresses := make(map[string]struct{})

		// 收集唯一的写入地址
		for key := range tx.LocalPut {
			uniqueAddresses[key] = struct{}{}
		}

		// 为每个地址计算最大序列号
		for address := range uniqueAddresses {
			if addr, exists := a.addresses[address]; exists {
				writeSeq := addr.writeUnits.max_seq
				readSeq := addr.readUnits.max_seq
				// 取最大值
				if writeSeq > seq {
					seq = writeSeq
				}
				if readSeq > seq {
					seq = readSeq
				}
			}
		}

		// 设置交易序列号并加入到交易列表
		tx.Sequenceid = seq
		a.txList = append(a.txList, tx)
	}
}

func (a *AddressBasedConflictGraph) extractTxsList() []*OptmeTransaction {
	return a.txList
}

// extractAbortList 提取中止交易列表（高效版本）
func (a *AddressBasedConflictGraph) extractAbortList() []*OptmeTransaction {
	var abortedList []*OptmeTransaction
	var newTxList []*OptmeTransaction

	// 单次遍历，同时构建新列表和提取中止交易
	for _, tx := range a.txList {
		if tx.Aborted.Load() {
			abortedList = append(abortedList, tx)
		} else {
			newTxList = append(newTxList, tx)
		}
	}

	// 更新交易列表（只包含未中止的交易）
	a.txList = newTxList

	return abortedList
}
