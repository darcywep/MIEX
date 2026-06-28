package pilotfish

import (
	"Janus/baselines/common"
	lvm "Janus/core/evm"
	"Janus/tools"
	"sort"
	"time"

	"github.com/holiman/uint256"
)

type pilotfishTransaction struct {
	inner          *common.BasicTransaction
	readKeys       []string
	writeKeys      []string
	accessKeys     []string
	selectedWorker int
	startTime      time.Time
}

func newPilotfishTransaction(inner *common.BasicTransaction) *pilotfishTransaction {
	tx := &pilotfishTransaction{inner: inner}
	tx.refreshReadWriteSet()
	return tx
}

func (tx *pilotfishTransaction) refreshReadWriteSet() {
	if tx == nil || tx.inner == nil || tx.inner.Vertex == nil {
		return
	}
	tx.readKeys = sortedMapKeys(tx.inner.Vertex.ReadKeys)
	tx.writeKeys = sortedMapKeys(tx.inner.Vertex.WriteKeys)
	tx.accessKeys = unionSortedKeys(tx.readKeys, tx.writeKeys)
}

func (tx *pilotfishTransaction) chooseExecutionWorker(workerCount int) int {
	if workerCount <= 0 {
		return 0
	}
	if len(tx.writeKeys) > 0 {
		return ownerForKey(tx.writeKeys[0], workerCount)
	}
	if len(tx.readKeys) > 0 {
		return ownerForKey(tx.readKeys[0], workerCount)
	}
	if tx == nil || tx.inner == nil {
		return 0
	}
	return int(tx.inner.Txid % uint32(workerCount))
}

func executePilotfishEthTransaction(tx *pilotfishTransaction, levm *lvm.LEVM) {
	if tx == nil || tx.inner == nil || tx.inner.EthTx == nil {
		return
	}
	ethTx := tx.inner.EthTx
	if tools.ExecuteSimulatedTransaction(ethTx) {
		return
	}
	if levm == nil || ethTx.From() == nil || ethTx.To() == nil {
		return
	}
	_, err := levm.CallContract(*ethTx.From(), *ethTx.To(), ethTx.Data(), new(uint256.Int).SetUint64(0))
	tools.PanicError("Pilotfish transaction execute", err)
}

func sortedMapKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func unionSortedKeys(readKeys, writeKeys []string) []string {
	seen := make(map[string]struct{}, len(readKeys)+len(writeKeys))
	keys := make([]string, 0, len(readKeys)+len(writeKeys))
	for _, key := range readKeys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for _, key := range writeKeys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
