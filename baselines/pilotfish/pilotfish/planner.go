package pilotfish

import (
	"hash/fnv"
	"sort"
	"sync"
)

type pilotfishQueueOp int

const (
	pilotfishRead pilotfishQueueOp = iota
	pilotfishWrite
)

type pendingEntry struct {
	op  pilotfishQueueOp
	txs []*pilotfishTransaction
}

type pilotfishExecutionPlan struct {
	allTxs     []*pilotfishTransaction
	queues     map[string][]*pendingEntry
	indegree   map[*pilotfishTransaction]int
	dependents map[*pilotfishTransaction][]*pilotfishTransaction
	remaining  int
	workers    int
}

func buildPilotfishPlan(txs []*pilotfishTransaction, workerCount int) *pilotfishExecutionPlan {
	workers := normalizeWorkerCount(workerCount)
	plan := &pilotfishExecutionPlan{
		allTxs:     txs,
		queues:     make(map[string][]*pendingEntry),
		indegree:   make(map[*pilotfishTransaction]int, len(txs)),
		dependents: make(map[*pilotfishTransaction][]*pilotfishTransaction, len(txs)),
		remaining:  len(txs),
		workers:    workers,
	}

	for _, tx := range txs {
		tx.refreshReadWriteSet()
		tx.selectedWorker = tx.chooseExecutionWorker(workers)
		plan.indegree[tx] = 0
	}

	for _, tx := range txs {
		writeSet := make(map[string]struct{}, len(tx.writeKeys))
		for _, key := range tx.writeKeys {
			writeSet[key] = struct{}{}
			plan.appendWrite(key, tx)
		}
		for _, key := range tx.readKeys {
			if _, writesSameKey := writeSet[key]; writesSameKey {
				continue
			}
			plan.appendRead(key, tx)
		}
	}

	plan.buildDependencies()
	return plan
}

func (p *pilotfishExecutionPlan) appendWrite(key string, tx *pilotfishTransaction) {
	p.queues[key] = append(p.queues[key], &pendingEntry{
		op:  pilotfishWrite,
		txs: []*pilotfishTransaction{tx},
	})
}

func (p *pilotfishExecutionPlan) appendRead(key string, tx *pilotfishTransaction) {
	queue := p.queues[key]
	if len(queue) > 0 && queue[len(queue)-1].op == pilotfishRead {
		queue[len(queue)-1].txs = append(queue[len(queue)-1].txs, tx)
		p.queues[key] = queue
		return
	}
	p.queues[key] = append(queue, &pendingEntry{
		op:  pilotfishRead,
		txs: []*pilotfishTransaction{tx},
	})
}

func (p *pilotfishExecutionPlan) buildDependencies() {
	keys := make([]string, 0, len(p.queues))
	for key := range p.queues {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	seenEdges := make(map[*pilotfishTransaction]map[*pilotfishTransaction]struct{})
	for _, key := range keys {
		queue := p.queues[key]
		for idx := 1; idx < len(queue); idx++ {
			prev := queue[idx-1]
			curr := queue[idx]
			for _, from := range prev.txs {
				for _, to := range curr.txs {
					p.addDependency(from, to, seenEdges)
				}
			}
		}
	}
}

func (p *pilotfishExecutionPlan) addDependency(from, to *pilotfishTransaction, seen map[*pilotfishTransaction]map[*pilotfishTransaction]struct{}) {
	if from == nil || to == nil || from == to {
		return
	}
	if seen[from] == nil {
		seen[from] = make(map[*pilotfishTransaction]struct{})
	}
	if _, ok := seen[from][to]; ok {
		return
	}
	seen[from][to] = struct{}{}
	p.dependents[from] = append(p.dependents[from], to)
	p.indegree[to]++
}

type pilotfishScheduler struct {
	mu    sync.Mutex
	cond  *sync.Cond
	plan  *pilotfishExecutionPlan
	ready [][]*pilotfishTransaction
}

func newPilotfishScheduler(plan *pilotfishExecutionPlan) *pilotfishScheduler {
	scheduler := &pilotfishScheduler{
		plan:  plan,
		ready: make([][]*pilotfishTransaction, plan.workers),
	}
	scheduler.cond = sync.NewCond(&scheduler.mu)
	for _, tx := range plan.allTxs {
		if plan.indegree[tx] == 0 {
			workerID := tx.selectedWorker
			scheduler.ready[workerID] = append(scheduler.ready[workerID], tx)
		}
	}
	return scheduler
}

func (s *pilotfishScheduler) next(workerID int) *pilotfishTransaction {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if s.plan.remaining == 0 {
			return nil
		}
		if len(s.ready[workerID]) > 0 {
			tx := s.ready[workerID][0]
			s.ready[workerID] = s.ready[workerID][1:]
			return tx
		}
		s.cond.Wait()
	}
}

func (s *pilotfishScheduler) complete(tx *pilotfishTransaction) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, dependent := range s.plan.dependents[tx] {
		s.plan.indegree[dependent]--
		if s.plan.indegree[dependent] == 0 {
			workerID := dependent.selectedWorker
			s.ready[workerID] = append(s.ready[workerID], dependent)
		}
	}
	s.plan.remaining--
	s.cond.Broadcast()
}

func ownerForKey(key string, workerCount int) int {
	if workerCount <= 0 {
		return 0
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32() % uint32(workerCount))
}

func normalizeWorkerCount(workerCount int) int {
	if workerCount <= 0 {
		return 1
	}
	return workerCount
}
