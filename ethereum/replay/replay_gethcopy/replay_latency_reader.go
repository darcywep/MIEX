package replay_gethcopy

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"Janus/ethereum/replay/replay_config"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
)

// BlockLatencyRecord 暴露给读取函数调用方使用，对应一个区块主表 value。
// 结构是 blockNumber -> txs，每个 tx 直接使用 txLatencyRecord。
type BlockLatencyRecord = blockLatencyValue

// ContractLatencyAggregate 暴露给读取函数调用方使用，表示一个合约函数的累计平均 latency。
type ContractLatencyAggregate = contractLatencyAggregate

const replayLatencySummaryKey = "summary_latency"
const replayLatencyReadTestBlockLimit = 1

// replayLatencySummaryValue 是写入 summary_latency 的统计结果。
// avg/min/max/lower_avg 仍然使用 LatencyDB 中的 ns 单位；greater/lower 字段表示交易数量。
type replayLatencySummaryValue struct {
	AvgLatencyNS              float64           `json:"avg_latencyt"`
	MinLatencyNS              float64           `json:"min"`
	MaxLatencyNS              float64           `json:"max"`
	LowerAvgLatencyNS         float64           `json:"lower_avg_latency"`
	GreaterAvgLatencyCnt      uint64            `json:"greater_avg_latency"`
	GreaterLowerAvgLatencyCnt uint64            `json:"greater_lower_avg_latency"`
	LowerLowerAvgLatencyCnt   uint64            `json:"lower_lower_avg_latency"`
	LatencyBuckets            map[string]uint64 `json:"-"`
}

// MarshalJSON 将 latency 分桶作为 summary_latency 的顶层字段写出，例如 latency0_10: 10。
func (s replayLatencySummaryValue) MarshalJSON() ([]byte, error) {
	fields := map[string]interface{}{
		"avg_latencyt":              s.AvgLatencyNS,
		"min":                       s.MinLatencyNS,
		"max":                       s.MaxLatencyNS,
		"lower_avg_latency":         s.LowerAvgLatencyNS,
		"greater_avg_latency":       s.GreaterAvgLatencyCnt,
		"greater_lower_avg_latency": s.GreaterLowerAvgLatencyCnt,
		"lower_lower_avg_latency":   s.LowerLowerAvgLatencyCnt,
	}
	for key, count := range s.LatencyBuckets {
		if count == 0 {
			continue
		}
		fields[key] = count
	}
	return json.Marshal(fields)
}

// UnmarshalJSON 读取 summary_latency 时把 latency0_10 这类动态字段还原到 LatencyBuckets。
func (s *replayLatencySummaryValue) UnmarshalJSON(data []byte) error {
	fields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*s = replayLatencySummaryValue{}
	for key, value := range fields {
		switch key {
		case "avg_latencyt":
			if err := json.Unmarshal(value, &s.AvgLatencyNS); err != nil {
				return err
			}
		case "min":
			if err := json.Unmarshal(value, &s.MinLatencyNS); err != nil {
				return err
			}
		case "max":
			if err := json.Unmarshal(value, &s.MaxLatencyNS); err != nil {
				return err
			}
		case "lower_avg_latency":
			if err := json.Unmarshal(value, &s.LowerAvgLatencyNS); err != nil {
				return err
			}
		case "greater_avg_latency":
			if err := json.Unmarshal(value, &s.GreaterAvgLatencyCnt); err != nil {
				return err
			}
		case "greater_lower_avg_latency":
			if err := json.Unmarshal(value, &s.GreaterLowerAvgLatencyCnt); err != nil {
				return err
			}
		case "lower_lower_avg_latency":
			if err := json.Unmarshal(value, &s.LowerLowerAvgLatencyCnt); err != nil {
				return err
			}
		default:
			if !strings.HasPrefix(key, "latency") {
				continue
			}
			var count uint64
			if err := json.Unmarshal(value, &count); err != nil {
				return err
			}
			if count == 0 {
				continue
			}
			if s.LatencyBuckets == nil {
				s.LatencyBuckets = make(map[string]uint64)
			}
			s.LatencyBuckets[key] = count
		}
	}
	return nil
}

// ReplayLatencyReader 持有只读 Pebble 句柄。
// 调用方批量读取多个区块或多个合约时，应复用同一个 reader，避免重复 open/close LatencyDB。
type ReplayLatencyReader struct {
	db   *pebble.DB
	path string
}

// NewReplayLatencyReader 以只读模式打开项目目录下的 LatencyDB Pebble。
func NewReplayLatencyReader() (*ReplayLatencyReader, error) {
	db, path, err := openReplayLatencyDB(true)
	if err != nil {
		return nil, err
	}
	return &ReplayLatencyReader{db: db, path: path}, nil
}

// Close 关闭 ReplayLatencyReader 持有的 Pebble 句柄。
func (r *ReplayLatencyReader) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

// Path 返回当前 reader 打开的 latency Pebble 路径。
func (r *ReplayLatencyReader) Path() string {
	if r == nil {
		return ""
	}
	return r.path
}

// ReadReplayBlockLatency 按区块号读取整个区块的 latency/rw 主表 value。
// reader 由调用方传入，便于连续读取多个区块时复用同一个 Pebble 句柄。
func ReadReplayBlockLatency(reader *ReplayLatencyReader, blockNumber uint64) (*BlockLatencyRecord, error) {
	if reader == nil || reader.db == nil {
		return nil, fmt.Errorf("ReplayLatencyReader 不能为空")
	}
	data, err := replayLatencyGet(reader.db, latencyBlockKey(blockNumber))
	if err != nil {
		return nil, err
	}
	return blockLatencyValueFromJSON(data)
}

// ReadReplayContractLatencies 按合约地址读取该合约下所有 methodSelector 的平均 latency。
// 合约平均值 key 是 contractAddress_methodSelector，因此这里扫描 <contractAddress>_ 前缀。
func ReadReplayContractLatencies(reader *ReplayLatencyReader, contractAddress string) (map[string]*ContractLatencyAggregate, error) {
	if reader == nil || reader.db == nil {
		return nil, fmt.Errorf("ReplayLatencyReader 不能为空")
	}
	address := normalizeContractAddress(contractAddress)
	if address == "" {
		return nil, fmt.Errorf("合约地址不能为空")
	}
	iter, err := newReplayLatencyIterator(reader.db, address+"_")
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	aggregates := make(map[string]*ContractLatencyAggregate)
	for iter.First(); iter.Valid(); iter.Next() {
		aggregate := new(contractLatencyAggregate)
		if err := json.Unmarshal(iter.Value(), aggregate); err != nil {
			return nil, fmt.Errorf("解析合约 latency 记录失败: %w", err)
		}
		aggregates[aggregate.MethodSelector] = aggregate
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return aggregates, nil
}

// ReadReplayContractMethodLatency 按合约地址和 methodSelector 读取该合约函数的平均 latency。
func ReadReplayContractMethodLatency(reader *ReplayLatencyReader, contractAddress, methodSelector string) (*ContractLatencyAggregate, error) {
	if reader == nil || reader.db == nil {
		return nil, fmt.Errorf("ReplayLatencyReader 不能为空")
	}
	key := contractLatencyKey(contractAddress, methodSelector)
	if key == "" {
		return nil, fmt.Errorf("contractAddress_methodSelector 不能为空")
	}
	data, err := replayLatencyGet(reader.db, key)
	if err != nil {
		return nil, err
	}
	aggregate := new(contractLatencyAggregate)
	if err := json.Unmarshal(data, aggregate); err != nil {
		return nil, fmt.Errorf("解析合约 latency 记录失败: %w", err)
	}
	return aggregate, nil
}

// UpdateReplayLatencyStatistics 从已有 LatencyDB 中读取每个区块的 txs，计算区块级和全局 latency summary 并写回。
// 区块仍写回原 blockNumber key，保留 block_number 和 txs，只新增 summary_latency 字段。
// 全局 summary 写入 key=summary_latency，value 包含 avg/min/max/lower_avg、相应数量统计和 latency 分桶。
func UpdateReplayLatencyStatistics() error {
	db, path, err := openReplayLatencyDB(false)
	if err != nil {
		return err
	}
	defer db.Close()

	startBlockNumber := replay_config.StartBlockNumber.Uint64()
	finishBlockNumber := replay_config.FinishBlockNumber.Uint64()
	addSpan := replay_config.AddSpan.Uint64()
	if addSpan == 0 {
		return fmt.Errorf("replay_config.AddSpan 不能为 0")
	}

	globalSummary := &replayLatencySummaryValue{}
	totalLatencyNS := float64(0)
	totalTxCount := uint64(0)

	fmt.Printf("\n========== Replay Latency Summary Update ==========\n")
	fmt.Printf("LatencyDB: %s\n", path)
	fmt.Printf("Blocks: [%d, %d)\n", startBlockNumber, finishBlockNumber)
	for blockNumber := startBlockNumber; blockNumber < finishBlockNumber; blockNumber += addSpan {
		blockValue, err := readReplayBlockLatencyValue(db, blockNumber)
		if err != nil {
			return fmt.Errorf("读取区块 latency 失败 block=%d: %w", blockNumber, err)
		}
		blockSummary, blockTotalLatencyNS, blockTxCount := summarizeReplayTxLatencies(blockValue.Txs)
		blockValue.SummaryLatency = blockSummary
		if err := writeReplayLatencyJSON(db, latencyBlockKey(blockNumber), blockValue); err != nil {
			return fmt.Errorf("写回区块 summary_latency 失败 block=%d: %w", blockNumber, err)
		}

		totalLatencyNS += blockTotalLatencyNS
		totalTxCount += blockTxCount
		updateGlobalLatencyMinMax(globalSummary, blockSummary, totalTxCount == blockTxCount)
		mergeReplayLatencyBuckets(globalSummary, blockSummary)
		fmt.Printf("[Block %d] tx_count=%d avg_us=%.2f lower_avg_us=%.2f min_us=%.2f max_us=%.2f greater_avg=%d greater_lower_avg=%d lower_lower_avg=%d buckets=%s\n",
			blockValue.BlockNumber,
			blockTxCount,
			blockSummary.AvgLatencyNS/1000,
			blockSummary.LowerAvgLatencyNS/1000,
			blockSummary.MinLatencyNS/1000,
			blockSummary.MaxLatencyNS/1000,
			blockSummary.GreaterAvgLatencyCnt,
			blockSummary.GreaterLowerAvgLatencyCnt,
			blockSummary.LowerLowerAvgLatencyCnt,
			formatReplayLatencyBuckets(blockSummary.LatencyBuckets),
		)
	}

	if totalTxCount > 0 {
		globalSummary.AvgLatencyNS = totalLatencyNS / float64(totalTxCount)
	}
	greaterAvg, lowerAvgLatencyNS, greaterLowerAvg, lowerLowerAvg, err := calculateReplayLatencyAroundAverage(db, startBlockNumber, finishBlockNumber, addSpan, globalSummary.AvgLatencyNS)
	if err != nil {
		return err
	}
	globalSummary.GreaterAvgLatencyCnt = greaterAvg
	globalSummary.LowerAvgLatencyNS = lowerAvgLatencyNS
	globalSummary.GreaterLowerAvgLatencyCnt = greaterLowerAvg
	globalSummary.LowerLowerAvgLatencyCnt = lowerLowerAvg
	if err := writeReplayLatencyJSON(db, replayLatencySummaryKey, globalSummary); err != nil {
		return fmt.Errorf("写回全局 summary_latency 失败: %w", err)
	}

	fmt.Printf("[Summary] tx_count=%d avg_us=%.2f lower_avg_us=%.2f min_us=%.2f max_us=%.2f greater_avg=%d greater_lower_avg=%d lower_lower_avg=%d buckets=%s\n",
		totalTxCount,
		globalSummary.AvgLatencyNS/1000,
		globalSummary.LowerAvgLatencyNS/1000,
		globalSummary.MinLatencyNS/1000,
		globalSummary.MaxLatencyNS/1000,
		globalSummary.GreaterAvgLatencyCnt,
		globalSummary.GreaterLowerAvgLatencyCnt,
		globalSummary.LowerLowerAvgLatencyCnt,
		formatReplayLatencyBuckets(globalSummary.LatencyBuckets),
	)
	fmt.Printf("===================================================\n\n")
	return nil
}

// TestReadReplayLatency 从 replay_config 配置的区块范围读取 latency/rw，
// 最后选择第一个迭代到的合约函数，再通过正式读取函数打印该合约的 latency 信息。
// 这是 replay 写入后的本地读测试入口，reader 只打开一次，避免连续读取多个区块时反复 open Pebble。
func TestReadReplayLatency() error {
	reader, err := NewReplayLatencyReader()
	if err != nil {
		return err
	}
	defer reader.Close()

	startBlockNumber := replay_config.StartBlockNumber.Uint64()
	finishBlockNumber := replay_config.FinishBlockNumber.Uint64()
	addSpan := replay_config.AddSpan.Uint64()
	if addSpan == 0 {
		return fmt.Errorf("replay_config.AddSpan 不能为 0")
	}

	fmt.Printf("\n========== Replay Latency Read Test ==========\n")
	fmt.Printf("LatencyDB: %s\n", reader.Path())
	fmt.Printf("Blocks: [%d, %d)\n", startBlockNumber, finishBlockNumber)
	globalData, err := replayLatencyGet(reader.db, replayLatencySummaryKey)
	if err != nil {
		return fmt.Errorf("读取全局 summary_latency 失败: %w", err)
	}
	globalSummary := new(replayLatencySummaryValue)
	if err := json.Unmarshal(globalData, globalSummary); err != nil {
		return fmt.Errorf("解析全局 summary_latency 失败: %w", err)
	}
	fmt.Printf("[Summary] summary_latency avg_us=%.2f lower_avg_us=%.2f min_us=%.2f max_us=%.2f greater_avg=%d greater_lower_avg=%d lower_lower_avg=%d buckets=%s\n",
		globalSummary.AvgLatencyNS/1000,
		globalSummary.LowerAvgLatencyNS/1000,
		globalSummary.MinLatencyNS/1000,
		globalSummary.MaxLatencyNS/1000,
		globalSummary.GreaterAvgLatencyCnt,
		globalSummary.GreaterLowerAvgLatencyCnt,
		globalSummary.LowerLowerAvgLatencyCnt,
		formatReplayLatencyBuckets(globalSummary.LatencyBuckets),
	)

	printedBlockCount := uint64(0)
	for blockNumber := startBlockNumber; blockNumber < finishBlockNumber && printedBlockCount < replayLatencyReadTestBlockLimit; blockNumber += addSpan {
		blockValue, err := ReadReplayBlockLatency(reader, blockNumber)
		if err != nil {
			return fmt.Errorf("读取区块 latency 失败 block=%d: %w", blockNumber, err)
		}
		fmt.Printf("\n[Block %d] tx_count=%d\n", blockValue.BlockNumber, len(blockValue.Txs))
		if blockValue.SummaryLatency != nil {
			fmt.Printf("  summary_latency avg_us=%.2f lower_avg_us=%.2f min_us=%.2f max_us=%.2f greater_avg=%d greater_lower_avg=%d lower_lower_avg=%d buckets=%s\n",
				blockValue.SummaryLatency.AvgLatencyNS/1000,
				blockValue.SummaryLatency.LowerAvgLatencyNS/1000,
				blockValue.SummaryLatency.MinLatencyNS/1000,
				blockValue.SummaryLatency.MaxLatencyNS/1000,
				blockValue.SummaryLatency.GreaterAvgLatencyCnt,
				blockValue.SummaryLatency.GreaterLowerAvgLatencyCnt,
				blockValue.SummaryLatency.LowerLowerAvgLatencyCnt,
				formatReplayLatencyBuckets(blockValue.SummaryLatency.LatencyBuckets),
			)
		}
		for _, tx := range blockValue.Txs {
			if tx == nil {
				continue
			}
			fmt.Printf("  txid=%d txhash=%s latency_us=%.2f read=%v write=%v",
				tx.TxIndex, tx.TxHash, tx.LatencyNS/1000, tx.ReadAddresses, tx.WriteAddresses)
			if tx.Error != "" {
				fmt.Printf(" error=%s", tx.Error)
			}
			fmt.Printf("\n")
		}
		printedBlockCount++
	}

	contractAddress, methodSelector, err := firstReplayLatencyContractMethod(reader)
	if err != nil {
		return err
	}
	aggregates, err := ReadReplayContractLatencies(reader, contractAddress)
	if err != nil {
		return fmt.Errorf("读取合约 latency 失败 contract=%s: %w", contractAddress, err)
	}
	fmt.Printf("\n[First Contract] address=%s method_count=%d\n", contractAddress, len(aggregates))
	for selector, aggregate := range aggregates {
		fmt.Printf("  selector=%s count=%d avg_latency_ns=%.2f avg_inclusive_latency_ns=%.2f total_latency_ns=%.2f total_inclusive_latency_ns=%.2f\n",
			selector,
			aggregate.Count,
			aggregate.AverageLatencyNS,
			aggregate.AverageInclusiveLatencyNS,
			aggregate.TotalLatencyNS,
			aggregate.TotalInclusiveLatencyNS,
		)
	}
	aggregate, err := ReadReplayContractMethodLatency(reader, contractAddress, methodSelector)
	if err != nil {
		return fmt.Errorf("读取合约方法 latency 失败 contract=%s selector=%s: %w", contractAddress, methodSelector, err)
	}
	fmt.Printf("[First Contract Method] key=%s count=%d avg_latency_ns=%.2f avg_inclusive_latency_ns=%.2f\n",
		aggregate.Key,
		aggregate.Count,
		aggregate.AverageLatencyNS,
		aggregate.AverageInclusiveLatencyNS,
	)
	fmt.Printf("==============================================\n\n")
	return nil
}

// blockLatencyValueFromJSON 从 <blockNumber> 的 JSON value 还原区块交易数组。
func blockLatencyValueFromJSON(value []byte) (*blockLatencyValue, error) {
	blockValue := new(blockLatencyValue)
	if err := json.Unmarshal(value, blockValue); err != nil {
		return nil, fmt.Errorf("解析区块 latency/rw 记录失败: %w", err)
	}
	if blockValue.Txs == nil {
		blockValue.Txs = make([]*txLatencyRecord, 0)
	}
	for _, tx := range blockValue.Txs {
		if tx == nil {
			continue
		}
		// BlockNumber 不落盘，读取区块 value 后补齐，方便调用方知道该交易来自哪个区块。
		tx.BlockNumber = blockValue.BlockNumber
	}
	return blockValue, nil
}

// replayLatencyGet 使用原生 Pebble Get，并在关闭 closer 前复制 value。
// Pebble 返回的 value 只在 closer 关闭前有效，复制后调用方才能安全解析 JSON。
func replayLatencyGet(db *pebble.DB, key string) ([]byte, error) {
	data, closer, err := db.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	value := make([]byte, len(data))
	copy(value, data)
	return value, nil
}

// readReplayBlockLatencyValue 直接从原生 Pebble 读取一个区块的 blockLatencyValue。
func readReplayBlockLatencyValue(db *pebble.DB, blockNumber uint64) (*blockLatencyValue, error) {
	data, err := replayLatencyGet(db, latencyBlockKey(blockNumber))
	if err != nil {
		return nil, err
	}
	return blockLatencyValueFromJSON(data)
}

// summarizeReplayTxLatencies 计算一组交易的平均、最小、最大以及高于/低于平均值的数量。
func summarizeReplayTxLatencies(txs []*txLatencyRecord) (*replayLatencySummaryValue, float64, uint64) {
	summary := &replayLatencySummaryValue{}
	totalLatencyNS := float64(0)
	txCount := uint64(0)
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		txCount++
		totalLatencyNS += tx.LatencyNS
		if txCount == 1 || tx.LatencyNS < summary.MinLatencyNS {
			summary.MinLatencyNS = tx.LatencyNS
		}
		if txCount == 1 || tx.LatencyNS > summary.MaxLatencyNS {
			summary.MaxLatencyNS = tx.LatencyNS
		}
		addReplayLatencyBucket(summary, tx.LatencyNS)
	}
	if txCount > 0 {
		summary.AvgLatencyNS = totalLatencyNS / float64(txCount)
	}
	lowerAvgTotalLatencyNS := float64(0)
	lowerAvgTxCount := uint64(0)
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		if tx.LatencyNS > summary.AvgLatencyNS {
			summary.GreaterAvgLatencyCnt++
		}
		if tx.LatencyNS < summary.AvgLatencyNS {
			lowerAvgTxCount++
			lowerAvgTotalLatencyNS += tx.LatencyNS
		}
	}
	if lowerAvgTxCount > 0 {
		summary.LowerAvgLatencyNS = lowerAvgTotalLatencyNS / float64(lowerAvgTxCount)
		for _, tx := range txs {
			if tx == nil {
				continue
			}
			if tx.LatencyNS > summary.LowerAvgLatencyNS {
				summary.GreaterLowerAvgLatencyCnt++
			}
			if tx.LatencyNS < summary.LowerAvgLatencyNS {
				summary.LowerLowerAvgLatencyCnt++
			}
		}
	}
	return summary, totalLatencyNS, txCount
}

// addReplayLatencyBucket 按 10us 宽度统计交易 latency 分布，只记录出现过交易的区间。
func addReplayLatencyBucket(summary *replayLatencySummaryValue, latencyNS float64) {
	if summary.LatencyBuckets == nil {
		summary.LatencyBuckets = make(map[string]uint64)
	}
	summary.LatencyBuckets[replayLatencyBucketKey(latencyNS)]++
}

// mergeReplayLatencyBuckets 把单区块 latency 分布合并到全局 summary。
func mergeReplayLatencyBuckets(globalSummary, blockSummary *replayLatencySummaryValue) {
	if blockSummary == nil || len(blockSummary.LatencyBuckets) == 0 {
		return
	}
	if globalSummary.LatencyBuckets == nil {
		globalSummary.LatencyBuckets = make(map[string]uint64)
	}
	for key, count := range blockSummary.LatencyBuckets {
		globalSummary.LatencyBuckets[key] += count
	}
}

// replayLatencyBucketKey 把纳秒 latency 映射到 [N,N+10)us 区间，例如 latency10_20。
func replayLatencyBucketKey(latencyNS float64) string {
	latencyUS := int(latencyNS / 1000)
	if latencyUS < 0 {
		latencyUS = 0
	}
	lower := (latencyUS / 10) * 10
	upper := lower + 10
	return fmt.Sprintf("latency%d_%d", lower, upper)
}

// formatReplayLatencyBuckets 按 key 排序后格式化 latency 分桶，便于读测试输出检查。
func formatReplayLatencyBuckets(buckets map[string]uint64) string {
	if len(buckets) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	builder.WriteString("{")
	for i, key := range keys {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("%s:%d", key, buckets[key]))
	}
	builder.WriteString("}")
	return builder.String()
}

// updateGlobalLatencyMinMax 将区块 min/max 合并到全局 summary。
func updateGlobalLatencyMinMax(globalSummary, blockSummary *replayLatencySummaryValue, firstNonEmptyBlock bool) {
	if blockSummary == nil || (blockSummary.MinLatencyNS == 0 && blockSummary.MaxLatencyNS == 0) {
		return
	}
	if firstNonEmptyBlock || blockSummary.MinLatencyNS < globalSummary.MinLatencyNS {
		globalSummary.MinLatencyNS = blockSummary.MinLatencyNS
	}
	if firstNonEmptyBlock || blockSummary.MaxLatencyNS > globalSummary.MaxLatencyNS {
		globalSummary.MaxLatencyNS = blockSummary.MaxLatencyNS
	}
}

// calculateReplayLatencyAroundAverage 先计算低于 avg_latency 的平均时延 lower_avg_latency，
// 再统计大于 avg_latency、大于 lower_avg_latency 和小于 lower_avg_latency 的交易数量。
func calculateReplayLatencyAroundAverage(db *pebble.DB, startBlockNumber, finishBlockNumber, addSpan uint64, avgLatencyNS float64) (uint64, float64, uint64, uint64, error) {
	greaterAvg := uint64(0)
	lowerAvgTotalLatencyNS := float64(0)
	lowerAvgTxCount := uint64(0)
	for blockNumber := startBlockNumber; blockNumber < finishBlockNumber; blockNumber += addSpan {
		blockValue, err := readReplayBlockLatencyValue(db, blockNumber)
		if err != nil {
			return 0, 0, 0, 0, fmt.Errorf("读取区块 latency 失败 block=%d: %w", blockNumber, err)
		}
		for _, tx := range blockValue.Txs {
			if tx == nil {
				continue
			}
			if tx.LatencyNS > avgLatencyNS {
				greaterAvg++
			}
			if tx.LatencyNS < avgLatencyNS {
				lowerAvgTxCount++
				lowerAvgTotalLatencyNS += tx.LatencyNS
			}
		}
	}
	if lowerAvgTxCount == 0 {
		return greaterAvg, 0, 0, 0, nil
	}

	lowerAvgLatencyNS := lowerAvgTotalLatencyNS / float64(lowerAvgTxCount)
	greaterLowerAvg := uint64(0)
	lowerLowerAvg := uint64(0)
	for blockNumber := startBlockNumber; blockNumber < finishBlockNumber; blockNumber += addSpan {
		blockValue, err := readReplayBlockLatencyValue(db, blockNumber)
		if err != nil {
			return 0, 0, 0, 0, fmt.Errorf("读取区块 latency 失败 block=%d: %w", blockNumber, err)
		}
		for _, tx := range blockValue.Txs {
			if tx == nil {
				continue
			}
			if tx.LatencyNS > lowerAvgLatencyNS {
				greaterLowerAvg++
			}
			if tx.LatencyNS < lowerAvgLatencyNS {
				lowerLowerAvg++
			}
		}
	}
	return greaterAvg, lowerAvgLatencyNS, greaterLowerAvg, lowerLowerAvg, nil
}

// writeReplayLatencyJSON 将统计结果以 JSON 写回 LatencyDB。
func writeReplayLatencyJSON(db *pebble.DB, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return db.Set([]byte(key), data, pebble.Sync)
}

// firstReplayLatencyContractMethod 返回 LatencyDB 中第一个迭代到的合约函数 key。
// 区块 key 是纯数字，合约 key 是 contractAddress_methodSelector，因此找到第一个合法下划线 key 即可。
func firstReplayLatencyContractMethod(reader *ReplayLatencyReader) (string, string, error) {
	iter, err := reader.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return "", "", err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if key == replayLatencySummaryKey {
			continue
		}
		parts := strings.SplitN(key, "_", 2)
		if len(parts) != 2 {
			continue
		}
		contractAddress := normalizeContractAddress(parts[0])
		methodSelector := normalizeMethodSelector(parts[1])
		if contractAddress == "" || methodSelector == "" {
			continue
		}
		return contractAddress, methodSelector, nil
	}
	if err := iter.Error(); err != nil {
		return "", "", err
	}
	return "", "", fmt.Errorf("LatencyDB 中没有找到合约 latency 记录")
}

// newReplayLatencyIterator 构造只扫描指定前缀的原生 Pebble iterator。
func newReplayLatencyIterator(db *pebble.DB, prefix string) (*pebble.Iterator, error) {
	prefixBytes := []byte(prefix)
	return db.NewIter(&pebble.IterOptions{
		LowerBound: prefixBytes,
		UpperBound: replayLatencyPrefixUpperBound(prefixBytes),
	})
}

// replayLatencyPrefixUpperBound 计算 Pebble 前缀迭代的右边界。
// 例如前缀 abc 的右边界是 abd，iterator 范围 [abc, abd) 正好覆盖所有 abc* key。
func replayLatencyPrefixUpperBound(prefix []byte) (limit []byte) {
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c == 0xff {
			continue
		}
		limit = make([]byte, i+1)
		copy(limit, prefix)
		limit[i] = c + 1
		break
	}
	return limit
}

// normalizeContractAddress 统一合约地址格式，和 trace 中 address.Hex() 生成的 key 保持一致。
func normalizeContractAddress(contractAddress string) string {
	address := strings.TrimSpace(contractAddress)
	if address == "" {
		return ""
	}
	if common.IsHexAddress(address) {
		return common.HexToAddress(address).Hex()
	}
	return address
}

// normalizeMethodSelector 统一 methodSelector 格式。
// 普通 ABI selector 使用 0x 前缀小写十六进制；constructor/fallback 保持文本标记。
func normalizeMethodSelector(methodSelector string) string {
	selector := strings.ToLower(strings.TrimSpace(methodSelector))
	if selector == "" {
		return ""
	}
	if selector == "constructor" || selector == "fallback" {
		return selector
	}
	if !strings.HasPrefix(selector, "0x") {
		selector = "0x" + selector
	}
	return selector
}

// contractLatencyKey 生成合约平均值的业务 key：contractAddress_methodSelector。
func contractLatencyKey(contractAddress, methodSelector string) string {
	address := normalizeContractAddress(contractAddress)
	selector := normalizeMethodSelector(methodSelector)
	if address == "" || selector == "" {
		return ""
	}
	return address + "_" + selector
}
