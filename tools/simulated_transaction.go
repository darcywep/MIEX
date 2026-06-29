package tools

import (
	"Janus/ethereum/core/types"
	"math"
	"sort"
	"sync/atomic"
	"time"
)

var simulatedTransactionSpinSink uint64

// NormalizeSimulationLatencyNS 将 LatencyDB 中 float64 形式的 ns 延迟转为 int64。
// 真实负载执行只需要纳秒级忙等，遇到非法值时统一收敛到 0 或 MaxInt64。
func NormalizeSimulationLatencyNS(latencyNS float64) int64 {
	if latencyNS <= 0 || math.IsNaN(latencyNS) {
		return 0
	}
	if latencyNS >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(latencyNS + 0.5)
}

// ExecuteSimulatedTransaction 在交易带有 LatencyDB 模拟信息时执行 CPU 忙等。
// 返回 true 表示已经完成模拟执行，调用方不应再进入真实 EVM。
func ExecuteSimulatedTransaction(tx *types.Transaction) bool {
	if tx == nil || !tx.IsSimulation() {
		return false
	}
	busyWaitSimulationLatency(tx.SimulationLatency())
	return true
}

// SimulatedTransactionCost 返回模拟交易的执行代价。
// Janus 的 MWIS/abort 逻辑需要一个 cost，这里直接使用 LatencyDB 的 ns latency。
func SimulatedTransactionCost(tx *types.Transaction) float64 {
	if tx == nil || !tx.IsSimulation() {
		return 0
	}
	return float64(tx.SimulationLatencyNS)
}

// TransactionReadWriteSet 返回交易执行后的读写集。
// 真实负载模拟交易优先使用 LatencyDB 的 Address 级读写集；普通合成交易保持原 SmallBank 规则。
func TransactionReadWriteSet(tx *types.Transaction) (map[string]struct{}, map[string]struct{}) {
	readSet := make(map[string]struct{})
	writeSet := make(map[string]struct{})
	if tx == nil {
		return readSet, writeSet
	}
	if tx.IsSimulation() {
		for _, key := range tx.SimulationReadSet() {
			addRWKey(readSet, key)
		}
		for _, key := range tx.SimulationWriteSet() {
			addRWKey(writeSet, key)
		}
		return readSet, writeSet
	}

	if len(tx.ReadKeys) > 0 || len(tx.WriteKeys) > 0 {
		for _, key := range tx.ReadKeys {
			addRWKey(readSet, key)
		}
		for _, key := range tx.WriteKeys {
			addRWKey(writeSet, key)
		}
		return readSet, writeSet
	}

	if from := tx.From(); from != nil {
		addRWKey(readSet, from.String())
		addRWKey(writeSet, from.String())
	}
	addRWKey(readSet, tx.SmallBankTo.String())
	addRWKey(writeSet, tx.SmallBankTo.String())
	return readSet, writeSet
}

// FillStringReadWriteSet 将交易读写集填入 baseline 使用的 string map。
// 该函数会先清空目标 map，避免重执行或 abort 重试时残留旧读写集。
func FillStringReadWriteSet(tx *types.Transaction, readKeys, writeKeys map[string]string) {
	clearStringValueMap(readKeys)
	clearStringValueMap(writeKeys)
	readSet, writeSet := TransactionReadWriteSet(tx)
	for key := range readSet {
		if readKeys != nil {
			readKeys[key] = "value"
		}
	}
	for key := range writeSet {
		if writeKeys != nil {
			writeKeys[key] = "value"
		}
	}
}

// FillTransactionReadWriteKeys 将交易读写集填入 SChain 使用的 ReadKeys/WriteKeys 切片。
func FillTransactionReadWriteKeys(tx *types.Transaction) {
	if tx == nil {
		return
	}
	readSet, writeSet := TransactionReadWriteSet(tx)
	tx.ReadKeys = sortedRWKeys(readSet)
	tx.WriteKeys = sortedRWKeys(writeSet)
}

// busyWaitSimulationLatency 通过空循环持续占用 CPU，模拟交易真实执行时延。
// 这里不调用 sleep/gosched，因为实验要求交易执行期间必须一直占用 CPU。
func busyWaitSimulationLatency(latency time.Duration) {
	if latency <= 0 {
		return
	}
	deadline := time.Now().Add(latency)
	var spin uint64
	for time.Now().Before(deadline) {
		spin++
	}
	atomic.AddUint64(&simulatedTransactionSpinSink, spin&1)
}

func addRWKey(set map[string]struct{}, key string) {
	if key == "" {
		return
	}
	set[key] = struct{}{}
}

func sortedRWKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func clearStringValueMap(values map[string]string) {
	for key := range values {
		delete(values, key)
	}
}
