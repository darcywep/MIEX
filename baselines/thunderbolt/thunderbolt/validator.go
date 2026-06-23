package thunderbolt

import (
	lvm "Janus/core/evm"
	"fmt"
	"sync"
)

type thunderboltValidationScheduler struct {
	mu        sync.Mutex
	cond      *sync.Cond
	indegree  map[*thunderboltTransaction]int
	remaining int
	ready     []*thunderboltTransaction
	failed    bool
}

func newThunderboltValidationScheduler(plan *thunderboltExecutionPlan) *thunderboltValidationScheduler {
	scheduler := &thunderboltValidationScheduler{
		indegree:  cloneIndegree(plan.indegree),
		remaining: len(plan.allTxs),
	}
	scheduler.cond = sync.NewCond(&scheduler.mu)
	for _, tx := range plan.allTxs {
		if scheduler.indegree[tx] == 0 {
			scheduler.ready = append(scheduler.ready, tx)
		}
	}
	return scheduler
}

func (s *thunderboltValidationScheduler) next() *thunderboltTransaction {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if s.failed || s.remaining == 0 {
			return nil
		}
		if len(s.ready) > 0 {
			tx := s.ready[0]
			s.ready = s.ready[1:]
			return tx
		}
		s.cond.Wait()
	}
}

func (s *thunderboltValidationScheduler) complete(plan *thunderboltExecutionPlan, tx *thunderboltTransaction) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, dependent := range plan.dependents[tx] {
		s.indegree[dependent]--
		if s.indegree[dependent] == 0 {
			s.ready = append(s.ready, dependent)
		}
	}
	s.remaining--
	s.cond.Broadcast()
}

func (s *thunderboltValidationScheduler) fail() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = true
	s.cond.Broadcast()
}

func validateThunderboltTransaction(tx *thunderboltTransaction, levm *lvm.LEVM) error {
	executeThunderboltEthTransaction(tx, levm)
	readKeys, writeKeys := readWriteKeysFromEthTx(tx.inner.EthTx)
	if !sameStringSet(readKeys, tx.readKeys) {
		return fmt.Errorf("read set mismatch tx=%d preplay=%v validation=%v", tx.inner.Txid, tx.readKeys, readKeys)
	}
	if !sameStringSet(writeKeys, tx.writeKeys) {
		return fmt.Errorf("write set mismatch tx=%d preplay=%v validation=%v", tx.inner.Txid, tx.writeKeys, writeKeys)
	}
	return nil
}
