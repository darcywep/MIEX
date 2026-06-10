package optme_paper

import (
	janusCommon "Janus/baselines/common"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/holiman/uint256"
)

// Run 是论文版 OPTME baseline 的统一入口。
// 该实现不复用旧 baselines/optme/optme，避免旧实现中的预约表和统计口径影响新的实验结果。
func Run(blockTxs []types.Transactions, levm *lvm.LEVM) [][]float64 {
	fmt.Println("=== Run OptME Paper ===")

	start := time.Now()
	txGenerator := janusCommon.NewTxGenerator(janusConfig.AllBlocksTxSum, janusConfig.BlockSize)
	blocks := txGenerator.GenerateWorkload(blockTxs)

	stats := janusCommon.NewStatistics()
	engine := newOptMEPaper(blocks, stats, janusConfig.AllThreadNum, levm)
	engine.start()
	engine.close()

	elapsed := time.Since(start)
	commitCount := stats.CommitCount.Load()
	execCount := stats.ExecCount.Load()
	tps := float64(commitCount) / elapsed.Seconds()

	fmt.Printf("OptME Paper CommitCount= %d\n", commitCount)
	fmt.Printf("OptME Paper ExecCount= %d\n", execCount)
	fmt.Printf("OptME Paper first_epoch_abort= %d\n", engine.firstEpochAbortCount)
	fmt.Printf("OptME Paper sequence_abort= %d\n", engine.sequenceAbortCount)
	fmt.Printf("OptME Paper future_epoch_txs= %d\n", engine.futureEpochTxCount)
	fmt.Printf("OptME Paper serial_fallback= %d\n", engine.serialFallbackCount)
	if int(commitCount) != janusConfig.AllBlocksTxSum {
		fmt.Printf("[OptME Paper warning] committed txs mismatch: committed=%d expected=%d\n", commitCount, janusConfig.AllBlocksTxSum)
	}
	fmt.Printf("OptME Paper TPS= %f\n", tps)

	return [][]float64{{tps}, {elapsed.Seconds()}}
}

type optMEPaper struct {
	blocks     []*janusCommon.Block
	statistics *janusCommon.Statistics
	pool       *janusCommon.ThreadPool
	serialLEVM *lvm.LEVM
	threadNum  int

	firstEpochAbortCount int
	sequenceAbortCount   int
	futureEpochTxCount   int
	serialFallbackCount  int
}

func newOptMEPaper(blocks []*janusCommon.Block, statistics *janusCommon.Statistics, threadNum int, levm *lvm.LEVM) *optMEPaper {
	if threadNum <= 0 {
		threadNum = 1
	}
	return &optMEPaper{
		blocks:     blocks,
		statistics: statistics,
		pool:       janusCommon.NewThreadPool(threadNum, levm),
		serialLEVM: levm.Copy(),
		threadNum:  threadNum,
	}
}

func (o *optMEPaper) close() {
	o.pool.Shutdown()
}

// start 按论文 Algorithm 1 运行：先模拟执行拿读写集，再构建 KDG 生成 schedule，最后按 schedule 执行/提交。
//
// 注意：这里刻意不做区块间并发。外层 block 循环是严格串行的；每个 block 内部虽然会并发执行
// simulation / execution 阶段，但每个阶段都通过 WaitGroup 做 barrier，当前区块完全完成后才进入下一区块。
func (o *optMEPaper) start() {
	for _, block := range o.blocks {
		o.runOneBlock(block)
	}
}

func (o *optMEPaper) runOneBlock(block *janusCommon.Block) {
	blockTxs := wrapBlockTransactions(block)
	if len(blockTxs) == 0 {
		o.statistics.JournalBlock()
		return
	}

	// Algorithm 1 line 1：在同一个快照上并发模拟执行，记录每笔交易访问过的 read/write keys。
	// 该阶段只用于获取调度所需的 RW set，不提交交易。
	o.executeParallel(blockTxs, true)

	// Algorithm 1 line 2-5：构建 key dependency graph，完成第一 epoch 排序，并把 abort 交易放入后续 epoch。
	schedule := buildPaperSchedule(blockTxs, o.threadNum)
	o.firstEpochAbortCount += schedule.firstEpochAbortCount
	o.sequenceAbortCount += schedule.sequenceAbortCount
	o.futureEpochTxCount += schedule.futureEpochTxCount

	// Algorithm 1 line 6：按照 schedule 真正执行/提交。
	// 第一 epoch 也重新执行一次，避免把 simulation 结果直接当作 commit，导致合成负载 TPS 被虚高。
	o.executeSchedule(schedule)
	o.statistics.JournalBlock()
}

func wrapBlockTransactions(block *janusCommon.Block) []*paperTransaction {
	txs := block.GetTxs()
	wrapped := make([]*paperTransaction, 0, len(txs))
	for _, tx := range txs {
		wrapped = append(wrapped, newPaperTransaction(tx))
	}
	return wrapped
}

type paperTransaction struct {
	tx *janusCommon.BasicTransaction

	id              uint32
	originalBlockID int
	originalTxID    int
	startTime       time.Time
	committed       bool
	aborted         bool
	sequence        uint32

	prevReadKeys  map[string]string
	prevWriteKeys map[string]string
	readKeys      map[string]string
	writeKeys     map[string]string
	writeUnits    []*paperUnit
}

func newPaperTransaction(tx *janusCommon.BasicTransaction) *paperTransaction {
	return &paperTransaction{
		tx:              tx,
		id:              tx.Txid,
		originalBlockID: tx.OriginalBlockID,
		originalTxID:    tx.OriginalTxID,
		prevReadKeys:    make(map[string]string),
		prevWriteKeys:   make(map[string]string),
		readKeys:        make(map[string]string),
		writeKeys:       make(map[string]string),
	}
}

// execute 执行交易并记录本次执行得到的读写集。
// 真实以太坊负载会走 LatencyDB 的 CPU 忙等模拟；合成负载会回退到 SmallBank/EVM 路径。
func (tx *paperTransaction) execute(levm *lvm.LEVM) {
	tx.startTime = time.Now()

	// 每一次 simulation / re-execution 都必须重新收集本次 RW set。
	// 如果不清空，future epoch 里 rwUnchanged() 可能因为残留 key 被误判，进而把本应 fallback 的交易并发提交。
	tx.resetExecutionRW()

	if !tools.ExecuteSimulatedTransaction(tx.tx.EthTx) {
		_, err := levm.CallContract(*tx.tx.EthTx.From(), *tx.tx.EthTx.To(), tx.tx.EthTx.Data(), new(uint256.Int).SetUint64(0))
		tools.PanicError("OptME Paper Execute", err)
	}
	tools.FillStringReadWriteSet(tx.tx.EthTx, tx.tx.Vertex.ReadKeys, tx.tx.Vertex.WriteKeys)
	tools.FillStringReadWriteSet(tx.tx.EthTx, tx.readKeys, tx.writeKeys)
}

func (tx *paperTransaction) resetExecutionRW() {
	clearStringMap(tx.readKeys)
	clearStringMap(tx.writeKeys)
	clearStringMap(tx.tx.Vertex.ReadKeys)
	clearStringMap(tx.tx.Vertex.WriteKeys)
}

func (tx *paperTransaction) rememberSimulationRW() {
	tx.prevReadKeys = cloneStringMap(tx.readKeys)
	tx.prevWriteKeys = cloneStringMap(tx.writeKeys)
}

func (tx *paperTransaction) rwUnchanged() bool {
	return sameStringMap(tx.prevReadKeys, tx.readKeys) && sameStringMap(tx.prevWriteKeys, tx.writeKeys)
}

func (tx *paperTransaction) resetKDGState() {
	tx.aborted = false
	tx.sequence = 0
	tx.writeUnits = nil
}

func (tx *paperTransaction) resetFutureEpochState() {
	tx.aborted = false
	tx.sequence = 0
	tx.writeUnits = nil
}

func (tx *paperTransaction) isSorted() bool {
	return tx.sequence != 0 || tx.aborted
}

func (tx *paperTransaction) setWriteUnits(units []*paperUnit) {
	tx.writeUnits = append(tx.writeUnits[:0], units...)
}

// reorderable 对齐官方 artifact 的 Transaction::reorderable。
// 只有 write-only、写 unit 数量大于 1、且每个写 unit 都没有 wr dependency/co-location 的 abort 交易，才回填第一 epoch。
func (tx *paperTransaction) reorderable() bool {
	if len(tx.writeUnits) <= 1 {
		return false
	}
	for _, unit := range tx.writeUnits {
		if unit.degree != 0 || unit.coLocated {
			return false
		}
	}
	return true
}

func (o *optMEPaper) executeParallel(txs []*paperTransaction, rememberSimulation bool) {
	var wg sync.WaitGroup
	for _, tx := range txs {
		tx := tx
		wg.Add(1)
		o.pool.Enqueue(func(levm *lvm.LEVM) {
			defer wg.Done()
			tx.execute(levm)
			if rememberSimulation {
				tx.rememberSimulationRW()
			}
			o.statistics.JournalExecute()
		})
	}
	wg.Wait()
}

func (o *optMEPaper) executeSerial(tx *paperTransaction) {
	tx.execute(o.serialLEVM)
	o.statistics.JournalExecute()
}

type paperSchedule struct {
	firstEpochSequences [][]*paperTransaction
	futureEpochs        [][]*paperTransaction

	firstEpochAbortCount int
	sequenceAbortCount   int
	futureEpochTxCount   int
}

func buildPaperSchedule(txs []*paperTransaction, threadNum int) *paperSchedule {
	firstEpochSequences, aborted, firstEpochAbortCount, sequenceAbortCount := buildFirstEpochSequences(txs, threadNum)
	futureEpochs := buildFutureEpochs(aborted)
	return &paperSchedule{
		firstEpochSequences:  firstEpochSequences,
		futureEpochs:         futureEpochs,
		firstEpochAbortCount: firstEpochAbortCount,
		sequenceAbortCount:   sequenceAbortCount,
		futureEpochTxCount:   len(aborted),
	}
}

// buildFirstEpochSequences 对齐论文 Algorithm 1 的 KDG construction、topological sorting 和 schedule generation。
// KDG 内部的具体排序规则按官方 artifact 移植：read unit 取已排序最小 sequence，write unit 使用 FCW 处理 co-located updater。
func buildFirstEpochSequences(txs []*paperTransaction, threadNum int) ([][]*paperTransaction, []*paperTransaction, int, int) {
	for _, tx := range txs {
		tx.resetKDGState()
	}
	kdg := buildKDGParallel(txs, threadNum)
	firstEpochAbortCount := len(kdg.abortedTxs)
	sequenceAbortCount := kdg.assignSequences()
	kdg.reorder()
	return kdg.sequences(), kdg.abortedTransactions(), firstEpochAbortCount, sequenceAbortCount
}

type paperUnitType int

const (
	paperReadUnit paperUnitType = iota
	paperWriteUnit
)

type paperUnit struct {
	tx        *paperTransaction
	key       string
	unitType  paperUnitType
	coLocated bool
	degree    int
}

type paperAddress struct {
	key              string
	inDegree         int
	outDegree        int
	readMaxSeq       uint32
	writeMaxSeq      uint32
	firstUpdaterFlag bool
	writeUpdaterSeen bool
	readUnits        []*paperUnit
	writeUnits       []*paperUnit
}

type paperKDG struct {
	txs        []*paperTransaction
	abortedTxs []*paperTransaction
	addresses  map[string]*paperAddress
}

func newPaperKDG() *paperKDG {
	return &paperKDG{addresses: make(map[string]*paperAddress)}
}

// buildKDGParallel 按官方 artifact 的 divide-and-conquer 方式并行构建 KDG。
// 每个子图覆盖连续 txid 区间，merge 时将右子图 unit append 到左子图，保持同一 key 下 unit 的 txid 顺序。
func buildKDGParallel(txs []*paperTransaction, threadNum int) *paperKDG {
	if len(txs) == 0 {
		return newPaperKDG()
	}
	if threadNum <= 1 || len(txs) <= 1 {
		return constructKDGChunk(txs)
	}
	if threadNum > len(txs) {
		threadNum = len(txs)
	}
	chunkSize := (len(txs) + threadNum - 1) / threadNum
	partials := make([]*paperKDG, threadNum)

	var wg sync.WaitGroup
	for chunkID := 0; chunkID < threadNum; chunkID++ {
		start := chunkID * chunkSize
		end := start + chunkSize
		if end > len(txs) {
			end = len(txs)
		}
		if start >= end {
			partials[chunkID] = newPaperKDG()
			continue
		}
		wg.Add(1)
		go func(chunkID, start, end int) {
			defer wg.Done()
			partials[chunkID] = constructKDGChunk(txs[start:end])
		}(chunkID, start, end)
	}
	wg.Wait()

	for len(partials) > 1 {
		next := make([]*paperKDG, (len(partials)+1)/2)
		var mergeWG sync.WaitGroup
		for i := 0; i < len(partials); i += 2 {
			idx := i / 2
			if i+1 >= len(partials) {
				next[idx] = partials[i]
				continue
			}
			mergeWG.Add(1)
			go func(idx, left, right int) {
				defer mergeWG.Done()
				partials[left].merge(partials[right])
				next[idx] = partials[left]
			}(idx, i, i+1)
		}
		mergeWG.Wait()
		partials = next
	}
	return partials[0]
}

func constructKDGChunk(txs []*paperTransaction) *paperKDG {
	kdg := newPaperKDG()
	for _, tx := range txs {
		writeUnits := convertToUnits(tx, paperWriteUnit, tx.writeKeys, tx.readKeys)
		if kdg.checkUpdaterAlreadyExistInSameAddress(writeUnits) {
			if markTransactionAborted(tx) {
				kdg.abortedTxs = append(kdg.abortedTxs, tx)
			}
			continue
		}

		readUnits := convertToUnits(tx, paperReadUnit, tx.readKeys, nil)
		setWRDependencies(readUnits, writeUnits)
		tx.setWriteUnits(writeUnits)

		kdg.txs = append(kdg.txs, tx)
		kdg.addUnitsToAddress(readUnits)
		kdg.addUnitsToAddress(writeUnits)
	}
	return kdg
}

func convertToUnits(tx *paperTransaction, unitType paperUnitType, keys map[string]string, readSet map[string]string) []*paperUnit {
	units := make([]*paperUnit, 0, len(keys))
	for key := range keys {
		_, coLocated := readSet[key]
		units = append(units, &paperUnit{tx: tx, key: key, unitType: unitType, coLocated: coLocated})
	}
	sort.Slice(units, func(i, j int) bool {
		return units[i].key < units[j].key
	})
	return units
}

// setWRDependencies 对齐 artifact 的 _set_wr_dependencies。
// 同一交易内，只要 write unit 和 read unit 不属于同一个 key，就互相增加 degree；degree 只用于 address rank。
func setWRDependencies(readUnits, writeUnits []*paperUnit) {
	for _, writeUnit := range writeUnits {
		for _, readUnit := range readUnits {
			if readUnit.key == writeUnit.key {
				continue
			}
			readUnit.degree++
			writeUnit.degree++
		}
	}
}

func (k *paperKDG) checkUpdaterAlreadyExistInSameAddress(writeUnits []*paperUnit) bool {
	for _, unit := range writeUnits {
		if !unit.coLocated {
			continue
		}
		address := k.addresses[unit.key]
		if address != nil && address.firstUpdaterFlag {
			return true
		}
	}
	return false
}

func (k *paperKDG) addUnitsToAddress(units []*paperUnit) {
	for _, unit := range units {
		address := k.addresses[unit.key]
		if address == nil {
			address = &paperAddress{key: unit.key}
			k.addresses[unit.key] = address
		}
		address.addUnit(unit)
	}
}

func (address *paperAddress) addUnit(unit *paperUnit) {
	if unit.unitType == paperReadUnit {
		address.inDegree += unit.degree
		address.readUnits = append(address.readUnits, unit)
		return
	}
	if unit.coLocated {
		address.firstUpdaterFlag = true
	}
	address.outDegree += unit.degree
	address.writeUnits = append(address.writeUnits, unit)
}

func (k *paperKDG) merge(other *paperKDG) {
	for key, right := range other.addresses {
		left := k.addresses[key]
		if left == nil {
			k.addresses[key] = right
			continue
		}
		left.merge(right)
	}
	k.txs = append(k.txs, other.txs...)
	k.abortedTxs = append(k.abortedTxs, other.abortedTxs...)
}

func (address *paperAddress) merge(other *paperAddress) {
	address.inDegree += other.inDegree
	address.outDegree += other.outDegree
	address.readUnits = append(address.readUnits, other.readUnits...)
	address.writeUnits = append(address.writeUnits, other.writeUnits...)
	address.firstUpdaterFlag = address.firstUpdaterFlag || other.firstUpdaterFlag
}

func (k *paperKDG) assignSequences() int {
	abortCount := 0
	for _, address := range k.sortedAddresses() {
		address.sortReadUnits()
		abortCount += address.sortWriteUnits()
	}
	return abortCount
}

func (k *paperKDG) sortedAddresses() []*paperAddress {
	addresses := make([]*paperAddress, 0, len(k.addresses))
	for _, address := range k.addresses {
		addresses = append(addresses, address)
	}
	sort.Slice(addresses, func(i, j int) bool {
		if addresses[i].inDegree != addresses[j].inDegree {
			return addresses[i].inDegree < addresses[j].inDegree
		}
		if addresses[i].outDegree != addresses[j].outDegree {
			return addresses[i].outDegree > addresses[j].outDegree
		}
		return addresses[i].key < addresses[j].key
	})
	return addresses
}

// sortReadUnits 对齐 artifact 的 ReadUnits::sort。
// 已排序读 unit 为空时使用 sequence=1；否则未排序读 unit 继承已排序读 unit 的最小 sequence，同时记录最大 sequence。
func (address *paperAddress) sortReadUnits() {
	sorted, remaining := partitionUnitsBySorted(address.readUnits)
	minSeq, maxSeq := uint32(1), uint32(1)
	foundSorted := false
	for _, unit := range sorted {
		if unit.tx.aborted {
			continue
		}
		seq := unit.tx.sequence
		if !foundSorted || seq < minSeq {
			minSeq = seq
		}
		if !foundSorted || seq > maxSeq {
			maxSeq = seq
		}
		foundSorted = true
	}
	for _, unit := range remaining {
		unit.tx.sequence = minSeq
	}
	address.readMaxSeq = maxSeq
	address.readUnits = append(sorted, remaining...)
}

// sortWriteUnits 对齐 artifact 的 WriteUnits::sort，并使用 First Committer Wins。
// co-located updater 中第一个获得 readMaxSeq+1，其余 updater abort；随后 abort sequence 小于 readMaxSeq 的写 unit。
func (address *paperAddress) sortWriteUnits() int {
	abortCount := 0
	sorted, remaining := partitionUnitsBySorted(address.writeUnits)

	for _, unit := range sorted {
		if unit.tx.aborted || !unit.coLocated {
			continue
		}
		if !address.writeUpdaterSeen {
			address.readMaxSeq++
			unit.tx.sequence = address.readMaxSeq
			address.writeUpdaterSeen = true
			continue
		}
		if markTransactionAborted(unit.tx) {
			abortCount++
		}
	}

	for _, unit := range sorted {
		if unit.tx.aborted {
			continue
		}
		if unit.tx.sequence < address.readMaxSeq {
			if markTransactionAborted(unit.tx) {
				abortCount++
			}
		}
	}

	address.readMaxSeq++
	writeSeq := address.readMaxSeq
	writeSeqSet := make(map[uint32]struct{}, len(sorted)+len(remaining))
	for _, unit := range sorted {
		writeSeqSet[unit.tx.sequence] = struct{}{}
	}
	for _, unit := range remaining {
		for {
			if _, exists := writeSeqSet[writeSeq]; !exists {
				break
			}
			writeSeq++
		}
		unit.tx.sequence = writeSeq
		writeSeqSet[writeSeq] = struct{}{}
	}
	address.writeMaxSeq = writeSeq
	address.writeUnits = append(sorted, remaining...)
	return abortCount
}

func partitionUnitsBySorted(units []*paperUnit) ([]*paperUnit, []*paperUnit) {
	sortedUnits := make([]*paperUnit, 0, len(units))
	remainingUnits := make([]*paperUnit, 0, len(units))
	for _, unit := range units {
		if unit.tx.isSorted() {
			sortedUnits = append(sortedUnits, unit)
		} else {
			remainingUnits = append(remainingUnits, unit)
		}
	}
	return sortedUnits, remainingUnits
}

// reorder 对齐 artifact 的 AddressBasedConflictGraph::reorder。
// 它只把少量 write-only abort 交易回填第一 epoch，其余 abort 交易交给 Algorithm 2 做 inter-epoch reordering。
func (k *paperKDG) reorder() int {
	allAborted := k.extractAbortedTransactions()
	reorderTargets := make([]*paperTransaction, 0)
	aborted := make([]*paperTransaction, 0, len(allAborted))
	for _, tx := range allAborted {
		if tx.reorderable() {
			reorderTargets = append(reorderTargets, tx)
		} else {
			aborted = append(aborted, tx)
		}
	}
	k.abortedTxs = uniqueSortedTransactions(aborted)

	for _, tx := range reorderTargets {
		seq := uint32(0)
		seenAddress := make(map[string]struct{})
		for _, unit := range tx.writeUnits {
			if _, seen := seenAddress[unit.key]; seen {
				continue
			}
			seenAddress[unit.key] = struct{}{}
			address := k.addresses[unit.key]
			if address == nil {
				continue
			}
			if address.writeMaxSeq > seq {
				seq = address.writeMaxSeq
			}
			if address.readMaxSeq > seq {
				seq = address.readMaxSeq
			}
		}
		tx.aborted = false
		tx.sequence = seq
		k.txs = append(k.txs, tx)
	}
	return len(reorderTargets)
}

func (k *paperKDG) extractAbortedTransactions() []*paperTransaction {
	aborted := append([]*paperTransaction(nil), k.abortedTxs...)
	active := make([]*paperTransaction, 0, len(k.txs))
	for _, tx := range k.txs {
		if tx.aborted {
			aborted = append(aborted, tx)
			continue
		}
		active = append(active, tx)
	}
	k.txs = active
	k.abortedTxs = nil
	return uniqueSortedTransactions(aborted)
}

func (k *paperKDG) sequences() [][]*paperTransaction {
	sequenceMap := make(map[uint32][]*paperTransaction)
	sequenceIDs := make([]uint32, 0)
	seenSequence := make(map[uint32]struct{})
	for _, tx := range k.txs {
		sequenceMap[tx.sequence] = append(sequenceMap[tx.sequence], tx)
		if _, seen := seenSequence[tx.sequence]; !seen {
			seenSequence[tx.sequence] = struct{}{}
			sequenceIDs = append(sequenceIDs, tx.sequence)
		}
		tx.writeUnits = nil
	}
	sort.Slice(sequenceIDs, func(i, j int) bool {
		return sequenceIDs[i] < sequenceIDs[j]
	})

	sequences := make([][]*paperTransaction, 0, len(sequenceIDs))
	for _, seq := range sequenceIDs {
		sequence := uniqueSortedTransactions(sequenceMap[seq])
		if len(sequence) == 0 {
			continue
		}
		sequences = append(sequences, sequence)
	}
	return sequences
}

func (k *paperKDG) abortedTransactions() []*paperTransaction {
	aborted := uniqueSortedTransactions(k.abortedTxs)
	for _, tx := range aborted {
		tx.resetFutureEpochState()
	}
	return aborted
}

func markTransactionAborted(tx *paperTransaction) bool {
	if tx.aborted {
		return false
	}
	tx.aborted = true
	return true
}

// buildFutureEpochs 对应论文 Algorithm 2。
// 每个后续 epoch 只保留一个 sequence；交易按 txid 顺序寻找第一个与该 epoch write set 不发生 rw/write 冲突的位置。
func buildFutureEpochs(aborted []*paperTransaction) [][]*paperTransaction {
	epochWriteSets := make([]map[string]struct{}, 0)
	epochs := make([][]*paperTransaction, 0)
	for _, tx := range aborted {
		txRWKeys := unionRWKeys(tx)
		epoch := 0
		for epoch < len(epochWriteSets) && intersects(txRWKeys, epochWriteSets[epoch]) {
			epoch++
		}
		if epoch == len(epochWriteSets) {
			epochWriteSets = append(epochWriteSets, make(map[string]struct{}))
			epochs = append(epochs, make([]*paperTransaction, 0))
		}
		epochs[epoch] = append(epochs[epoch], tx)
		for key := range tx.writeKeys {
			epochWriteSets[epoch][key] = struct{}{}
		}
	}
	return epochs
}

func (o *optMEPaper) executeSchedule(schedule *paperSchedule) {
	for _, sequence := range schedule.firstEpochSequences {
		if len(sequence) == 0 {
			continue
		}
		// 第一 epoch 的 sequence 已经由 KDG 保证可并发提交，但仍需要执行阶段本身。
		// 原实现只 JournalCommit，会把 simulation 当成执行结果，容易高估 TPS。
		o.executeParallel(sequence, false)
		o.commitTransactions(sequence)
	}

	for _, epoch := range schedule.futureEpochs {
		o.executeFutureEpoch(epoch)
	}
}

func (o *optMEPaper) executeFutureEpoch(epoch []*paperTransaction) {
	if len(epoch) == 0 {
		return
	}

	// Algorithm 3 line 4：后续 epoch 必须重新执行，并重新记录本次 read/write keys。
	o.executeParallel(epoch, false)

	validTxs := make([]*paperTransaction, 0, len(epoch))
	invalidTxs := make([]*paperTransaction, 0)
	for _, tx := range epoch {
		if tx.rwUnchanged() {
			validTxs = append(validTxs, tx)
		} else {
			invalidTxs = append(invalidTxs, tx)
		}
	}

	if len(invalidTxs) > 0 {
		sortTransactions(invalidTxs)
		writeSet := unionWriteKeys(validTxs)
		stillInvalid := make([]*paperTransaction, 0)
		for _, tx := range invalidTxs {
			// Algorithm 3 line 11-16：invalid 交易只要当前 write keys 与已接受交易的 write set 不相交，仍可并发提交。
			if disjointWriteKeys(tx, writeSet) {
				validTxs = append(validTxs, tx)
				for key := range tx.writeKeys {
					writeSet[key] = struct{}{}
				}
			} else {
				stillInvalid = append(stillInvalid, tx)
			}
		}
		invalidTxs = stillInvalid
	}

	o.commitTransactions(validTxs)

	// Algorithm 3 fallback：仍无法验证的交易串行重执行并提交。
	for _, tx := range invalidTxs {
		o.executeSerial(tx)
		o.commitTransaction(tx)
		o.serialFallbackCount++
	}
}

func (o *optMEPaper) commitTransactions(txs []*paperTransaction) {
	for _, tx := range txs {
		o.commitTransaction(tx)
	}
}

func (o *optMEPaper) commitTransaction(tx *paperTransaction) {
	if tx.committed {
		return
	}
	tx.committed = true
	o.statistics.JournalCommit(uint32(time.Since(tx.startTime).Microseconds()))
}

func unionRWKeys(tx *paperTransaction) map[string]struct{} {
	result := make(map[string]struct{}, len(tx.readKeys)+len(tx.writeKeys))
	for key := range tx.readKeys {
		result[key] = struct{}{}
	}
	for key := range tx.writeKeys {
		result[key] = struct{}{}
	}
	return result
}

func unionWriteKeys(txs []*paperTransaction) map[string]struct{} {
	result := make(map[string]struct{})
	for _, tx := range txs {
		for key := range tx.writeKeys {
			result[key] = struct{}{}
		}
	}
	return result
}

func disjointWriteKeys(tx *paperTransaction, writeSet map[string]struct{}) bool {
	for key := range tx.writeKeys {
		if _, ok := writeSet[key]; ok {
			return false
		}
	}
	return true
}

func intersects(left, right map[string]struct{}) bool {
	for key := range left {
		if _, ok := right[key]; ok {
			return true
		}
	}
	return false
}

func sortTransactions(txs []*paperTransaction) {
	sort.Slice(txs, func(i, j int) bool {
		return txs[i].id < txs[j].id
	})
}

func uniqueSortedTransactions(txs []*paperTransaction) []*paperTransaction {
	if len(txs) == 0 {
		return nil
	}
	sortTransactions(txs)
	unique := make([]*paperTransaction, 0, len(txs))
	seen := make(map[*paperTransaction]struct{}, len(txs))
	for _, tx := range txs {
		if _, ok := seen[tx]; ok {
			continue
		}
		seen[tx] = struct{}{}
		unique = append(unique, tx)
	}
	return unique
}

func clearStringMap(values map[string]string) {
	for key := range values {
		delete(values, key)
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func sameStringMap(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftValue := range left {
		if rightValue, ok := right[key]; !ok || rightValue != leftValue {
			return false
		}
	}
	return true
}
