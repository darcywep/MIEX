package replay_gethcopy

import (
	"encoding/json"
	"fmt"
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

	for blockNumber := startBlockNumber; blockNumber < finishBlockNumber; blockNumber += addSpan {
		blockValue, err := ReadReplayBlockLatency(reader, blockNumber)
		if err != nil {
			return fmt.Errorf("读取区块 latency 失败 block=%d: %w", blockNumber, err)
		}
		fmt.Printf("\n[Block %d] tx_count=%d\n", blockValue.BlockNumber, len(blockValue.Txs))
		for _, tx := range blockValue.Txs {
			if tx == nil {
				continue
			}
			fmt.Printf("  txid=%d txhash=%s latency_ns=%.2f read=%v write=%v",
				tx.TxIndex, tx.TxHash, tx.LatencyNS, tx.ReadAddresses, tx.WriteAddresses)
			if tx.Error != "" {
				fmt.Printf(" error=%s", tx.Error)
			}
			fmt.Printf("\n")
		}
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
