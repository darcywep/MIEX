package utils

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Statistics struct {
	// Atomic counters for various metrics
	countCommit          uint64
	countExecution       uint64
	countOverhead        uint64
	countLatency         uint64
	countLatencyRollback uint64
	countLatencyReExec   uint64
	countBlock           uint64
	countRollback        uint64

	// Time tracking
	beginTime time.Time
	endTime   time.Time
}

// JournalCommit records a commit event with latency
func (s *Statistics) JournalCommit(latency uint64) {
	atomic.AddUint64(&s.countCommit, 1)
	atomic.AddUint64(&s.countLatency, latency)
}

// JournalCommitWithEndTime records a commit event with a specific end time
func (s *Statistics) JournalCommitWithEndTime(latency uint64, nEndTime time.Time) {
	atomic.AddUint64(&s.countCommit, 1)
	atomic.AddUint64(&s.countLatency, latency)
	s.endTime = nEndTime
}

// JournalExecute increments the count of executions
func (s *Statistics) JournalExecute() {
	atomic.AddUint64(&s.countExecution, 1)
}

// JournalOverheads records the overhead count
func (s *Statistics) JournalOverheads(count uint64) {
	atomic.AddUint64(&s.countOverhead, count)
}

// JournalRollback increments the rollback count
func (s *Statistics) JournalRollback(count uint64) {
	atomic.AddUint64(&s.countRollback, count)
}

// JournalBlock increments the block count
func (s *Statistics) JournalBlock() {
	atomic.AddUint64(&s.countBlock, 1)
}

// JournalRollbackExecution records latency during a rollback execution
func (s *Statistics) JournalRollbackExecution(latency uint64) {
	atomic.AddUint64(&s.countLatencyRollback, latency)
}

// JournalReExecution records latency during a re-execution
func (s *Statistics) JournalReExecution(latency uint64) {
	atomic.AddUint64(&s.countLatencyReExec, latency)
}

// Print generates a summary of the statistics
func (s *Statistics) Print() string {
	return fmt.Sprintf(
		"Commit: %d\nExecution: %d\nOverhead: %d\nLatency: %d\nLatency Rollback: %d\nReExecution Latency: %d\nBlock: %d\nRollback: %d\n",
		atomic.LoadUint64(&s.countCommit),
		atomic.LoadUint64(&s.countExecution),
		atomic.LoadUint64(&s.countOverhead),
		atomic.LoadUint64(&s.countLatency),
		atomic.LoadUint64(&s.countLatencyRollback),
		atomic.LoadUint64(&s.countLatencyReExec),
		atomic.LoadUint64(&s.countBlock),
		atomic.LoadUint64(&s.countRollback),
	)
}
