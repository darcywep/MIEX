package mvschedo

import (
	"Janus/baselines/common"
	lvm "Janus/core/evm"
	"Janus/tools"
	"sort"
	"sync"
	"time"

	"github.com/holiman/uint256"
)

type OperationType int

const (
	ReadOperation OperationType = iota
	WriteOperation
)

type ScheduledOperation struct {
	ID   int
	Key  string
	Type OperationType
}

func (op ScheduledOperation) conflictsWith(other ScheduledOperation) bool {
	return op.Type == WriteOperation || other.Type == WriteOperation
}

type MVSchedOTransaction struct {
	Inner           *common.BasicTransaction
	OriginalBlockID int
	OriginalTxID    int
	ArrivalID       uint32
	Timestamp       uint64
	LocalGet        map[string]string
	LocalPut        map[string]string
	Ops             []ScheduledOperation
	StartTime       time.Time

	depsMu       sync.Mutex
	dependencies map[*MVSchedOTransaction]struct{}

	statusMu  sync.Mutex
	statusCV  *sync.Cond
	committed bool
	aborted   bool
}

func NewMVSchedOTransaction(inner *common.BasicTransaction) *MVSchedOTransaction {
	tx := &MVSchedOTransaction{
		Inner:           inner,
		OriginalBlockID: inner.OriginalBlockID,
		OriginalTxID:    inner.OriginalTxID,
		ArrivalID:       inner.Txid,
		LocalGet:        make(map[string]string),
		LocalPut:        make(map[string]string),
		dependencies:    make(map[*MVSchedOTransaction]struct{}),
	}
	tx.statusCV = sync.NewCond(&tx.statusMu)
	return tx
}

func (tx *MVSchedOTransaction) RefreshReadWriteSet() {
	copyStringMap(tx.LocalGet, tx.Inner.Vertex.ReadKeys)
	copyStringMap(tx.LocalPut, tx.Inner.Vertex.WriteKeys)
	tx.RebuildOperations()
}

func (tx *MVSchedOTransaction) RebuildOperations() {
	tx.Ops = tx.Ops[:0]
	opID := 0

	readKeys := sortedKeys(tx.LocalGet)
	for _, key := range readKeys {
		tx.Ops = append(tx.Ops, ScheduledOperation{
			ID:   opID,
			Key:  key,
			Type: ReadOperation,
		})
		opID++
	}

	writeKeys := sortedKeys(tx.LocalPut)
	for _, key := range writeKeys {
		tx.Ops = append(tx.Ops, ScheduledOperation{
			ID:   opID,
			Key:  key,
			Type: WriteOperation,
		})
		opID++
	}
}

func (tx *MVSchedOTransaction) AddDependency(dep *MVSchedOTransaction) {
	if dep == nil || dep == tx {
		return
	}
	tx.depsMu.Lock()
	tx.dependencies[dep] = struct{}{}
	tx.depsMu.Unlock()
}

func (tx *MVSchedOTransaction) Dependencies() []*MVSchedOTransaction {
	tx.depsMu.Lock()
	defer tx.depsMu.Unlock()

	deps := make([]*MVSchedOTransaction, 0, len(tx.dependencies))
	for dep := range tx.dependencies {
		deps = append(deps, dep)
	}
	return deps
}

func (tx *MVSchedOTransaction) MarkCommitted() {
	tx.statusMu.Lock()
	tx.committed = true
	tx.aborted = false
	tx.statusCV.Broadcast()
	tx.statusMu.Unlock()
}

func (tx *MVSchedOTransaction) MarkAborted() {
	tx.statusMu.Lock()
	tx.aborted = true
	tx.committed = false
	tx.statusCV.Broadcast()
	tx.statusMu.Unlock()
}

func (tx *MVSchedOTransaction) WaitFinalStatus() bool {
	tx.statusMu.Lock()
	defer tx.statusMu.Unlock()

	for !tx.committed && !tx.aborted {
		tx.statusCV.Wait()
	}
	return tx.committed
}

func executeEthTransaction(tx *MVSchedOTransaction, levm *lvm.LEVM) {
	if tx == nil || tx.Inner == nil || tx.Inner.EthTx == nil {
		return
	}
	ethTx := tx.Inner.EthTx
	if tools.ExecuteSimulatedTransaction(ethTx) {
		return
	}
	_, err := levm.CallContract(*ethTx.From(), *ethTx.To(), ethTx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("MVSchedO transaction execute", err)
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func copyStringMap(dst, src map[string]string) {
	for key := range dst {
		delete(dst, key)
	}
	for key, value := range src {
		dst[key] = value
	}
}
