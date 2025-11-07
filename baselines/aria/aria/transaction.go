package aria

import (
	"sync/atomic"
	"time"
)

// -----------------------------
// Transaction: 事务类型
// -----------------------------
type Transaction struct {
	HyperId   uint64
	ReadKeys  map[string]struct{}
	WriteKeys map[string]struct{}
}

type AriaTransaction struct {
	Transaction
	id           uint64
	batchID      uint64
	flagConflict bool
	committed    atomic.Bool
	startTime    time.Time
	localGet     map[string]string
	localPut     map[string]string
}

func NewAriaTransaction(inner Transaction, id, batchID uint64) *AriaTransaction {
	tx := &AriaTransaction{
		Transaction: inner,
		id:          id,
		batchID:     batchID,
		localGet:    make(map[string]string),
		localPut:    make(map[string]string),
	}
	return tx
}
