package replay_gethcopy

import (
	"encoding/json"
	"fmt"
	"strings"

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
