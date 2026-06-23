package thunderbolt

import (
	"Janus/baselines/common"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"sort"
	"time"

	"github.com/holiman/uint256"
)

type thunderboltOperationType int

const (
	thunderboltRead thunderboltOperationType = iota
	thunderboltWrite
)

type thunderboltOperation struct {
	tx     *thunderboltTransaction
	opType thunderboltOperationType
	key    string
}

type thunderboltTransaction struct {
	inner        *common.BasicTransaction
	readKeys     []string
	writeKeys    []string
	preplayOrder int
	startTime    time.Time
}

func newThunderboltTransaction(inner *common.BasicTransaction) *thunderboltTransaction {
	tx := &thunderboltTransaction{inner: inner}
	tx.refreshReadWriteSet()
	return tx
}

func (tx *thunderboltTransaction) refreshReadWriteSet() {
	if tx == nil || tx.inner == nil || tx.inner.Vertex == nil {
		return
	}
	tx.readKeys = sortedMapKeys(tx.inner.Vertex.ReadKeys)
	tx.writeKeys = sortedMapKeys(tx.inner.Vertex.WriteKeys)
}

func (tx *thunderboltTransaction) operations() []thunderboltOperation {
	if tx == nil {
		return nil
	}
	ops := make([]thunderboltOperation, 0, len(tx.readKeys)+len(tx.writeKeys))
	for _, key := range tx.readKeys {
		ops = append(ops, thunderboltOperation{
			tx:     tx,
			opType: thunderboltRead,
			key:    key,
		})
	}
	for _, key := range tx.writeKeys {
		ops = append(ops, thunderboltOperation{
			tx:     tx,
			opType: thunderboltWrite,
			key:    key,
		})
	}
	return ops
}

func executeThunderboltEthTransaction(tx *thunderboltTransaction, levm *lvm.LEVM) {
	if tx == nil || tx.inner == nil || tx.inner.EthTx == nil {
		return
	}
	executeThunderboltRawTransaction(tx.inner.EthTx, levm)
}

func executeThunderboltRawTransaction(tx *types.Transaction, levm *lvm.LEVM) {
	if tx == nil {
		return
	}
	if tools.ExecuteSimulatedTransaction(tx) {
		return
	}
	if levm == nil || tx.From() == nil || tx.To() == nil {
		return
	}
	_, err := levm.CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("Thunderbolt transaction execute", err)
}

func readWriteKeysFromEthTx(tx *types.Transaction) ([]string, []string) {
	readSet, writeSet := tools.TransactionReadWriteSet(tx)
	return sortedStructKeys(readSet), sortedStructKeys(writeSet)
}

func sortedMapKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedStructKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
