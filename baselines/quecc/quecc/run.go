package quecc

import (
	"Janus/baselines/common"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/holiman/uint256"
)

type QueCC struct {
	statistics *common.Statistics
	blocks     []*common.Block
	numThreads int
	rangeCount int

	levm    *lvm.LEVM
	stateMu sync.Mutex

	durabilityMu  sync.Mutex
	durabilityLog *os.File
}

type queccTransaction struct {
	inner            *common.BasicTransaction
	priorityGroup    int
	rangeIDs         []int
	queues           []*executionQueue
	startTime        time.Time
	executing        bool
	completed        bool
	committed        bool
	commitDeps       int
	commitDependents []*queccTransaction
	undoLog          []undoEntry
	originalBlockID  int
	originalTxID     int
}

type undoEntry struct {
	key         string
	beforeValue string
}

type executionQueue struct {
	priorityGroup int
	rangeID       int
	tokens        []*queccTransaction
	head          int
}

type executionPlan struct {
	queues     [][]*executionQueue
	allTxs     []*queccTransaction
	indegree   map[*queccTransaction]int
	dependents map[*queccTransaction][]*queccTransaction
	remaining  int
}

type queccScheduler struct {
	mu    sync.Mutex
	cond  *sync.Cond
	plan  *executionPlan
	ready []*queccTransaction
}

type batchRuntime struct {
	mu         sync.Mutex
	lastWriter map[string]*queccTransaction
}

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) [][]float64 {
	fmt.Println("=== Run QueCC ===")

	start := time.Now()
	txGenerator := common.NewTxGenerator(janusConfig.AllBlocksTxSum, janusConfig.BlockSize)
	blocks := txGenerator.GenerateWorkload(blockTxs)

	stats := common.NewStatistics()
	quecc := NewQueCC(blocks, stats, janusConfig.AllThreadNum, levm)
	defer quecc.Close()
	fmt.Printf("QueCC worker threads: %d \n", quecc.numThreads)
	fmt.Printf("QueCC range queues per priority group: %d \n", quecc.rangeCount)
	if quecc.durabilityLog != nil {
		fmt.Printf("QueCC durability log: %s \n", quecc.durabilityLog.Name())
	}
	quecc.Start()

	elapsed := time.Since(start)
	committed := stats.CommitCount.Load()
	tps := 0.0
	if elapsed.Seconds() > 0 {
		tps = float64(committed) / elapsed.Seconds()
	}

	fmt.Printf("CommitCount= %d \n", committed)
	fmt.Printf("交易实际被执行总次数 %d \n", stats.ExecCount.Load())
	fmt.Printf("QueCC rollback count= %d \n", stats.RollbackCount.Load())
	fmt.Printf("交易处理吞吐(TPS)= %f \n", tps)
	fmt.Printf("QueCC Execution Time:     %-22v \n", elapsed)

	return [][]float64{{tps}, {elapsed.Seconds()}}
}

func NewQueCC(blocks []*common.Block, statistics *common.Statistics, numThreads int, levm *lvm.LEVM) *QueCC {
	workers := normalizeWorkerCount(numThreads)
	durabilityLog, err := os.CreateTemp("", "miex-quecc-durability-*.log")
	tools.PanicError("QueCC create durability log", err)
	return &QueCC{
		statistics:    statistics,
		blocks:        blocks,
		numThreads:    workers,
		rangeCount:    workers,
		levm:          levm.Copy(),
		durabilityLog: durabilityLog,
	}
}

func (q *QueCC) Close() {
	if q.durabilityLog == nil {
		return
	}
	err := q.durabilityLog.Close()
	tools.PanicError("QueCC close durability log", err)
}

func (q *QueCC) Start() {
	for blockID, block := range q.blocks {
		blockStart := time.Now()
		preStart := time.Now()
		txs := q.preExecuteBlock(block)
		preElapsed := time.Since(preStart)

		planStart := time.Now()
		plan := q.planBlock(txs)
		q.persistPlan(blockID, plan)
		planElapsed := time.Since(planStart)

		executeStart := time.Now()
		q.executePlan(plan)
		executeElapsed := time.Since(executeStart)

		commitStart := time.Now()
		q.commitPlan(plan)
		q.persistCommit(blockID, plan)
		commitElapsed := time.Since(commitStart)
		q.statistics.JournalBlock()

		if shouldPrintBlockProgress(blockID, len(q.blocks)) {
			fmt.Printf("[QueCC] block %d/%d txs=%d pre_execute=%v plan=%v execute=%v commit=%v total=%v commits=%d\n",
				blockID+1,
				len(q.blocks),
				len(txs),
				preElapsed,
				planElapsed,
				executeElapsed,
				commitElapsed,
				time.Since(blockStart),
				q.statistics.CommitCount.Load(),
			)
		}
	}
}

func (q *QueCC) preExecuteBlock(block *common.Block) []*queccTransaction {
	basicTxs := block.GetTxs()
	txs := make([]*queccTransaction, len(basicTxs))
	for idx, basicTx := range basicTxs {
		txs[idx] = &queccTransaction{
			inner:           basicTx,
			originalBlockID: basicTx.OriginalBlockID,
			originalTxID:    basicTx.OriginalTxID,
		}
	}

	jobs := make(chan *queccTransaction)
	var wg sync.WaitGroup
	for workerID := 0; workerID < q.numThreads; workerID++ {
		wg.Add(1)
		workerEVM := q.copyMasterEVM()
		go func(localEVM *lvm.LEVM) {
			defer wg.Done()
			for tx := range jobs {
				executeEthTransaction(tx.inner.EthTx, localEVM)
				tools.FillStringReadWriteSet(tx.inner.EthTx, tx.inner.Vertex.ReadKeys, tx.inner.Vertex.WriteKeys)
				tx.rebuildRanges(q.rangeCount)
				q.statistics.AddExecCount()
			}
		}(workerEVM)
	}

	for _, tx := range txs {
		jobs <- tx
	}
	close(jobs)
	wg.Wait()

	return txs
}

func (q *QueCC) planBlock(txs []*queccTransaction) *executionPlan {
	plannerCount := q.numThreads
	if len(txs) < plannerCount {
		plannerCount = len(txs)
	}
	if plannerCount == 0 {
		plannerCount = 1
	}

	plan := &executionPlan{
		queues:     make([][]*executionQueue, plannerCount),
		allTxs:     txs,
		remaining:  len(txs),
		indegree:   make(map[*queccTransaction]int, len(txs)),
		dependents: make(map[*queccTransaction][]*queccTransaction, len(txs)),
	}
	for _, tx := range txs {
		plan.indegree[tx] = 0
	}
	for pg := 0; pg < plannerCount; pg++ {
		plan.queues[pg] = make([]*executionQueue, q.rangeCount)
		for rangeID := 0; rangeID < q.rangeCount; rangeID++ {
			plan.queues[pg][rangeID] = &executionQueue{
				priorityGroup: pg,
				rangeID:       rangeID,
			}
		}
	}

	for pg := 0; pg < plannerCount; pg++ {
		start, end := chunkBounds(len(txs), plannerCount, pg)
		for _, tx := range txs[start:end] {
			tx.priorityGroup = pg
			tx.queues = tx.queues[:0]
			for _, rangeID := range tx.rangeIDs {
				queue := plan.queues[pg][rangeID]
				queue.tokens = append(queue.tokens, tx)
				tx.queues = append(tx.queues, queue)
			}
		}
	}

	plan.buildDependencies()
	return plan
}

func (q *QueCC) executePlan(plan *executionPlan) {
	if plan.remaining == 0 {
		return
	}

	runtime := newBatchRuntime()
	scheduler := newQueccScheduler(plan)
	var wg sync.WaitGroup
	for workerID := 0; workerID < q.numThreads; workerID++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				tx := scheduler.next()
				if tx == nil {
					return
				}
				q.executeScheduledTransaction(tx, runtime)
				scheduler.complete(tx)
			}
		}()
	}
	wg.Wait()
}

func (q *QueCC) executeScheduledTransaction(tx *queccTransaction, runtime *batchRuntime) {
	tx.startTime = time.Now()
	ethTx := tx.inner.EthTx
	runtime.recordAccess(tx)
	if ethTx == nil {
		q.statistics.AddExecCount()
		return
	}

	if ethTx.IsSimulation() {
		tools.ExecuteSimulatedTransaction(ethTx)
		q.statistics.AddExecCount()
		return
	}

	q.stateMu.Lock()
	executeEthTransaction(ethTx, q.levm)
	q.stateMu.Unlock()
	q.statistics.AddExecCount()
}

func (q *QueCC) commitPlan(plan *executionPlan) {
	remaining := len(plan.allTxs)
	for remaining > 0 {
		progressed := false
		for _, tx := range plan.allTxs {
			if tx.committed || tx.commitDeps > 0 {
				continue
			}
			tx.committed = true
			for _, dependent := range tx.commitDependents {
				dependent.commitDeps--
			}
			q.statistics.JournalCommit(uint32(time.Since(tx.startTime).Microseconds()))
			remaining--
			progressed = true
		}
		if !progressed {
			panic("QueCC commit dependency cycle")
		}
	}
}

func (q *QueCC) persistPlan(blockID int, plan *executionPlan) {
	if q.durabilityLog == nil || plan == nil {
		return
	}

	q.durabilityMu.Lock()
	defer q.durabilityMu.Unlock()

	_, err := fmt.Fprintf(q.durabilityLog, "PLAN_BEGIN block=%d txs=%d priority_groups=%d ranges=%d\n",
		blockID, len(plan.allTxs), len(plan.queues), q.rangeCount)
	tools.PanicError("QueCC persist plan begin", err)

	for _, tx := range plan.allTxs {
		_, err = fmt.Fprintf(q.durabilityLog, "PLAN_TX block=%d txid=%d original_block=%d original_tx=%d pg=%d ranges=",
			blockID, tx.inner.Txid, tx.originalBlockID, tx.originalTxID, tx.priorityGroup)
		tools.PanicError("QueCC persist plan tx", err)
		for idx, rangeID := range tx.rangeIDs {
			if idx > 0 {
				_, err = fmt.Fprint(q.durabilityLog, ",")
				tools.PanicError("QueCC persist plan tx separator", err)
			}
			_, err = fmt.Fprint(q.durabilityLog, rangeID)
			tools.PanicError("QueCC persist plan tx range", err)
		}
		_, err = fmt.Fprintln(q.durabilityLog)
		tools.PanicError("QueCC persist plan tx newline", err)
	}

	for pg, queues := range plan.queues {
		for _, queue := range queues {
			_, err = fmt.Fprintf(q.durabilityLog, "PLAN_EQ block=%d pg=%d range=%d len=%d txids=",
				blockID, pg, queue.rangeID, len(queue.tokens))
			tools.PanicError("QueCC persist plan eq", err)
			for idx, tx := range queue.tokens {
				if idx > 0 {
					_, err = fmt.Fprint(q.durabilityLog, ",")
					tools.PanicError("QueCC persist plan eq separator", err)
				}
				_, err = fmt.Fprint(q.durabilityLog, tx.inner.Txid)
				tools.PanicError("QueCC persist plan eq tx", err)
			}
			_, err = fmt.Fprintln(q.durabilityLog)
			tools.PanicError("QueCC persist plan eq newline", err)
		}
	}

	_, err = fmt.Fprintf(q.durabilityLog, "PLAN_END block=%d\n", blockID)
	tools.PanicError("QueCC persist plan end", err)
	err = q.durabilityLog.Sync()
	tools.PanicError("QueCC fsync plan", err)
}

func (q *QueCC) persistCommit(blockID int, plan *executionPlan) {
	if q.durabilityLog == nil || plan == nil {
		return
	}

	q.durabilityMu.Lock()
	defer q.durabilityMu.Unlock()

	_, err := fmt.Fprintf(q.durabilityLog, "COMMIT_BEGIN block=%d txs=%d\n", blockID, len(plan.allTxs))
	tools.PanicError("QueCC persist commit begin", err)
	for _, tx := range plan.allTxs {
		_, err = fmt.Fprintf(q.durabilityLog, "COMMIT_TX block=%d txid=%d committed=%t undo_entries=%d dependents=%d\n",
			blockID, tx.inner.Txid, tx.committed, len(tx.undoLog), len(tx.commitDependents))
		tools.PanicError("QueCC persist commit tx", err)
		for _, undo := range tx.undoLog {
			_, err = fmt.Fprintf(q.durabilityLog, "UNDO block=%d txid=%d key=%s before=%s\n",
				blockID, tx.inner.Txid, undo.key, undo.beforeValue)
			tools.PanicError("QueCC persist undo", err)
		}
	}
	_, err = fmt.Fprintf(q.durabilityLog, "COMMIT_END block=%d\n", blockID)
	tools.PanicError("QueCC persist commit end", err)
	err = q.durabilityLog.Sync()
	tools.PanicError("QueCC fsync commit", err)
}

func (q *QueCC) copyMasterEVM() *lvm.LEVM {
	q.stateMu.Lock()
	defer q.stateMu.Unlock()
	return q.levm.Copy()
}

func newQueccScheduler(plan *executionPlan) *queccScheduler {
	scheduler := &queccScheduler{plan: plan}
	scheduler.cond = sync.NewCond(&scheduler.mu)
	for _, tx := range plan.allTxs {
		if plan.indegree[tx] == 0 {
			scheduler.ready = append(scheduler.ready, tx)
		}
	}
	return scheduler
}

func newBatchRuntime() *batchRuntime {
	return &batchRuntime{
		lastWriter: make(map[string]*queccTransaction),
	}
}

func (rt *batchRuntime) recordAccess(tx *queccTransaction) {
	if tx == nil || tx.inner == nil || tx.inner.Vertex == nil {
		return
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	for key := range tx.inner.Vertex.ReadKeys {
		if writer := rt.lastWriter[key]; writer != nil && writer != tx {
			tx.commitDeps++
			writer.commitDependents = append(writer.commitDependents, tx)
		}
	}
	for key := range tx.inner.Vertex.WriteKeys {
		before := "initial"
		if writer := rt.lastWriter[key]; writer != nil {
			before = fmt.Sprintf("tx:%d", writer.inner.Txid)
		}
		tx.undoLog = append(tx.undoLog, undoEntry{key: key, beforeValue: before})
		rt.lastWriter[key] = tx
	}
}

func (s *queccScheduler) next() *queccTransaction {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if s.plan.remaining == 0 {
			return nil
		}
		if len(s.ready) > 0 {
			tx := s.ready[0]
			s.ready = s.ready[1:]
			tx.executing = true
			return tx
		}
		s.cond.Wait()
	}
}

func (s *queccScheduler) complete(tx *queccTransaction) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx.completed = true
	tx.executing = false
	for _, dependent := range s.plan.dependents[tx] {
		s.plan.indegree[dependent]--
		if s.plan.indegree[dependent] == 0 {
			s.ready = append(s.ready, dependent)
		}
	}
	s.plan.remaining--
	s.cond.Broadcast()
}

func (p *executionPlan) buildDependencies() {
	seenEdges := make(map[*queccTransaction]map[*queccTransaction]struct{})
	for pg := range p.queues {
		for _, queue := range p.queues[pg] {
			for idx := 1; idx < len(queue.tokens); idx++ {
				p.addDependency(queue.tokens[idx-1], queue.tokens[idx], seenEdges)
			}
		}
	}

	if len(p.queues) == 0 {
		return
	}
	rangeCount := len(p.queues[0])
	for rangeID := 0; rangeID < rangeCount; rangeID++ {
		var lastHigherPriorityTx *queccTransaction
		for pg := range p.queues {
			queue := p.queues[pg][rangeID]
			if len(queue.tokens) == 0 {
				continue
			}
			if lastHigherPriorityTx != nil {
				p.addDependency(lastHigherPriorityTx, queue.tokens[0], seenEdges)
			}
			lastHigherPriorityTx = queue.tokens[len(queue.tokens)-1]
		}
	}
}

func (p *executionPlan) addDependency(from, to *queccTransaction, seen map[*queccTransaction]map[*queccTransaction]struct{}) {
	if from == nil || to == nil || from == to {
		return
	}
	if seen[from] == nil {
		seen[from] = make(map[*queccTransaction]struct{})
	}
	if _, exists := seen[from][to]; exists {
		return
	}
	seen[from][to] = struct{}{}
	p.dependents[from] = append(p.dependents[from], to)
	p.indegree[to]++
}

func (tx *queccTransaction) rebuildRanges(rangeCount int) {
	rangeSet := make(map[int]struct{})
	for key := range tx.inner.Vertex.ReadKeys {
		rangeSet[keyToRange(key, rangeCount)] = struct{}{}
	}
	for key := range tx.inner.Vertex.WriteKeys {
		rangeSet[keyToRange(key, rangeCount)] = struct{}{}
	}
	if len(rangeSet) == 0 {
		rangeSet[keyToRange(fmt.Sprintf("tx:%d", tx.inner.Txid), rangeCount)] = struct{}{}
	}

	tx.rangeIDs = tx.rangeIDs[:0]
	for rangeID := range rangeSet {
		tx.rangeIDs = append(tx.rangeIDs, rangeID)
	}
	sort.Ints(tx.rangeIDs)
}

func executeEthTransaction(tx *types.Transaction, levm *lvm.LEVM) {
	if tx == nil {
		return
	}
	if tools.ExecuteSimulatedTransaction(tx) {
		return
	}
	_, err := levm.CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("QueCC transaction execute", err)
}

func keyToRange(key string, rangeCount int) int {
	if rangeCount <= 1 {
		return 0
	}
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum64() % uint64(rangeCount))
}

func normalizeWorkerCount(numThreads int) int {
	if numThreads <= 0 {
		return 1
	}
	return numThreads
}

func chunkBounds(total, chunks, chunk int) (int, int) {
	start := chunk * total / chunks
	end := (chunk + 1) * total / chunks
	return start, end
}

func shouldPrintBlockProgress(blockID, blockCount int) bool {
	if blockCount <= 20 {
		return true
	}
	if blockID == 0 || blockID+1 == blockCount {
		return true
	}
	return (blockID+1)%100 == 0
}
