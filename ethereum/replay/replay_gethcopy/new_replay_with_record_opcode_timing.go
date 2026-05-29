package replay_gethcopy

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"time"

	"Janus/ethereum/config"
	"Janus/ethereum/core/tracing"
	"Janus/ethereum/core/types"
	"Janus/ethereum/core/vm"
	"Janus/ethereum/database"
	"Janus/ethereum/replay/replay_config"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
)

const (
	// 256GB内存配置
	TrieMemoryLimit  = 128 * 1024 * 1024 * 1024 // 128GB - trie节点
	ImageMemoryLimit = 10 * 1024 * 1024 * 1024  // 10GB - 图像

	// 激进的刷盘策略（这些是关键）
	CommitInterval   = 50  // 每50个区块完全Commit（真正释放内存）
	CapInterval      = 20  // 每20个区块Cap（整理内存）
	ForceGCInterval  = 100 // 每100个区块强制GC
	MemCheckInterval = 10  // 每10个区块检查内存
)

const (
	// replay 当前按区块串行执行，VM 侧交易计数器使用 worker 0 即可。
	latencyWorkerID = 0
	// 区块主表 key 直接使用 blockNumber，不再拼交易下标或交易 hash。
	// Pebble 中的结构是：<blockNumber> -> txs(txid, txhash, latency, rw)。
	// replay latency 使用原生 Pebble batch 写入，本地维护批量提交阈值，避免依赖以太坊数据库接口常量。
	replayLatencyBatchSize = 100 * 1024
	// trie Cap 时预留一小段空间，原先复用数据库 batch 常量；这里改成本地常量，避免本文件依赖 ethdb。
	replayTrieCapReserve = 100 * 1024
)

var (
	// replayLatencyDBDirName 配置 replay latency Pebble 在项目根目录下的目录名。
	// 默认写入 <项目根目录>/LatencyDB；需要切换目录时只改这里，路径推导逻辑保持不变。
	replayLatencyDBDirName = "LatencyDB"
)

// txLatencyRecord 是 replay 内部使用的单笔交易 latency 摘要。
//
// LatencyNS 使用统一公式计算：
//
//	tx_wall_latency - evm_wall_latency + opcode_estimated_latency
//
// 普通 ETH 转账不会进入 EVM，因此 evm_wall_latency 为 0，LatencyNS 就是整笔交易真实耗时。
// 合约交易会先剥离真实 EVM 执行窗口，再加上 InstructionTimers 的指令估算值。
type txLatencyRecord struct {
	// BlockNumber 只在读取接口返回时补齐；区块 value 顶层已经保存 block_number，这里不重复落盘。
	BlockNumber    uint64   `json:"-"`
	TxIndex        int      `json:"txid"`
	TxHash         string   `json:"txhash"`
	LatencyNS      float64  `json:"latency_ns"`
	Error          string   `json:"error,omitempty"`
	ReadAddresses  []string `json:"read_addresses,omitempty"`
	WriteAddresses []string `json:"write_addresses,omitempty"`
}

// blockLatencyValue 是交易 latency 主表真正落盘的 value。
// key 只到区块号：<blockNumber>，value 中的 txs 数组保存该区块全部交易。
type blockLatencyValue struct {
	BlockNumber uint64             `json:"block_number"`
	Txs         []*txLatencyRecord `json:"txs"`
}

// contractLatencyAggregate 是某个 contractAddress_methodSelector 在整个 replay 区间内的累计平均。
//
// Total/AverageLatencyNS 只统计当前 frame 自己执行的 opcode。
// Inclusive 字段包含嵌套子调用，用来回答“带内部调用时，这个合约入口总共花了多久”。
type contractLatencyAggregate struct {
	Key                       string  `json:"key"`
	ContractAddress           string  `json:"contract_address"`
	MethodSelector            string  `json:"method_selector"`
	Count                     uint64  `json:"count"`
	TotalLatencyNS            float64 `json:"total_latency_ns"`
	AverageLatencyNS          float64 `json:"average_latency_ns"`
	TotalInclusiveLatencyNS   float64 `json:"total_inclusive_latency_ns"`
	AverageInclusiveLatencyNS float64 `json:"average_inclusive_latency_ns"`
}

// blockLatencyRecorder 负责接收一个区块 replay 过程中的 TxStart/TxEnd hook。
// 它把 state_processor 的交易事件和 VM 侧计数器连接起来。
type blockLatencyRecorder struct {
	blockNumber     uint64
	records         []*txLatencyRecord
	aggregates      map[string]*contractLatencyAggregate
	touched         map[string]struct{}
	currentTx       *types.Transaction
	currentIdx      int
	txStart         time.Time
	currentReadSet  map[string]struct{}
	currentWriteSet map[string]struct{}
}

// newReplayLatencyDB 打开专门存 replay latency 的 Pebble。
// 它和 chaindata 分离，避免测量结果污染以太坊原始数据库。
func newReplayLatencyDB() (*pebble.DB, string, error) {
	return openReplayLatencyDB(false)
}

// openReplayLatencyDB 根据 readonly 参数直接构建 latency Pebble。
// 这里不走项目数据库统一启动封装，避免复用以太坊主库启动路径；只创建 replay latency 自己的原生 Pebble。
// replay 写入路径使用读写模式，查询函数使用只读模式，避免误写统计库。
func openReplayLatencyDB(readonly bool) (*pebble.DB, string, error) {
	path := replayLatencyDBPath()
	if !readonly {
		// 原生 Pebble 不经过项目数据库封装，这里显式创建存放目录。
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, path, err
		}
	}
	db, err := pebble.Open(path, &pebble.Options{
		ReadOnly: readonly,
	})
	if err != nil {
		return nil, path, err
	}
	return db, path, nil
}

// replayLatencyDBPath 返回 latency Pebble 的输出路径。
// 路径固定在项目根目录下的 LatencyDB，不依赖程序从哪个目录启动。
func replayLatencyDBPath() string {
	_, filename, _, _ := runtime.Caller(0)
	// 当前文件在 ethereum/replay/replay_gethcopy 下，向上四级回到项目根目录。
	projectRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filename))))
	return filepath.Join(projectRoot, replayLatencyDBDirName)
}

// newBlockLatencyRecorder 复用跨区块的 aggregate map；
// records 和 touched 只记录当前区块，供本区块 Pebble batch 写入。
func newBlockLatencyRecorder(blockNumber uint64, aggregates map[string]*contractLatencyAggregate) *blockLatencyRecorder {
	return &blockLatencyRecorder{
		blockNumber: blockNumber,
		aggregates:  aggregates,
		touched:     make(map[string]struct{}),
	}
}

// hooks 返回交易级 latency 和读写集采集所需的 tracer。
// opcode 耗时仍在 interpreter.Run 内部采集；这里的 OnOpcode 只用于从 SLOAD/SSTORE/CALL* 等指令提取 Address 读写集。
func (r *blockLatencyRecorder) hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnTxStart:       r.onTxStart,
		OnTxEnd:         r.onTxEnd,
		OnOpcode:        r.onOpcode,
		OnBalanceChange: r.onBalanceChange,
		OnNonceChangeV2: r.onNonceChangeV2,
		OnCodeChangeV2:  r.onCodeChangeV2,
		OnStorageChange: r.onStorageChange,
	}
}

// onTxStart 在 state_processor 开始执行单笔交易时启动本交易的所有 latency 计数器。
// 交易 wall time 从这里开始，因此普通转账即使不进入 EVM 也能被统计到。
func (r *blockLatencyRecorder) onTxStart(_ *tracing.VMContext, tx *types.Transaction, from common.Address) {
	// 在 ApplyMessage 开始前记录交易 wall time 起点。
	// 这个区间覆盖普通转账、intrinsic gas/account 检查、状态访问和 EVM 调用。
	r.currentTx = tx
	r.currentIdx = len(r.records)
	r.txStart = time.Now()
	r.currentReadSet = make(map[string]struct{})
	r.currentWriteSet = make(map[string]struct{})
	// 交易执行前会读取发送方的余额和 nonce；普通转账不进入 EVM，也需要能形成读集。
	r.recordReadAddress(from)
	if to := tx.To(); to != nil {
		// 目标地址会在转账或合约调用路径中被检查，按 Address 粒度记入读集。
		r.recordReadAddress(*to)
	}
	// 统一打开本交易的 VM 侧采集器：
	// 1. 交易级 opcode 次数 -> opcode_latency_ns
	// 2. 合约 frame 内 opcode 次数 -> contractAddress_methodSelector 平均值
	// 3. 顶层 EVM 真实耗时 -> evm_wall_latency_ns
	vm.OpenTxLatencyTrace(latencyWorkerID)
}

// onTxEnd 在单笔交易执行结束时关闭计数器，并生成准备写入 Pebble 的交易记录。
// 这里会把整笔交易真实耗时拆成 non_evm_latency 和 evm_wall_latency。
func (r *blockLatencyRecorder) onTxEnd(_ *types.Receipt, err error) {
	if r.currentTx == nil {
		return
	}
	wallLatency := time.Since(r.txStart).Nanoseconds()
	traceResult := vm.CloseTxLatencyTrace(latencyWorkerID)
	// 交易耗时按需求去掉真实 EVM 执行窗口，避免普通转账在不进入 EVM 时算成 0。
	// 公式保持为：tx_wall_latency - evm_wall_latency + opcode_estimated_latency。
	nonEVMLatency := wallLatency - traceResult.EVMWallTimeNS
	if nonEVMLatency < 0 {
		nonEVMLatency = 0
	}
	latency := float64(nonEVMLatency) + traceResult.OpcodeLatencyNS
	readAddresses := sortedAddressSet(r.currentReadSet)
	writeAddresses := sortedAddressSet(r.currentWriteSet)
	for _, trace := range traceResult.ContractTraces {
		normalizeInclusiveLatency(trace)
		r.aggregateContractTrace(trace)
	}
	record := &txLatencyRecord{
		BlockNumber:    r.blockNumber,
		TxIndex:        r.currentIdx,
		TxHash:         r.currentTx.Hash().Hex(),
		LatencyNS:      latency,
		ReadAddresses:  readAddresses,
		WriteAddresses: writeAddresses,
	}
	if err != nil {
		record.Error = err.Error()
	}
	r.records = append(r.records, record)
	r.currentTx = nil
	r.txStart = time.Time{}
	r.currentReadSet = nil
	r.currentWriteSet = nil
}

// onOpcode 从 EVM opcode 中补充读写集。
// 当前按 Address 粒度统计：SLOAD 读取当前合约地址，SSTORE 写当前合约地址；
// BALANCE/EXTCODE*/CALL* 等会读取栈上目标地址。
func (r *blockLatencyRecorder) onOpcode(_ uint64, op byte, _, _ uint64, scope tracing.OpContext, _ []byte, _ int, err error) {
	if r.currentTx == nil || scope == nil || err != nil {
		return
	}
	switch vm.OpCode(op) {
	case vm.SLOAD:
		r.recordReadAddress(scope.Address())
	case vm.SSTORE:
		r.recordWriteAddress(scope.Address())
	case vm.BALANCE, vm.EXTCODESIZE, vm.EXTCODECOPY, vm.EXTCODEHASH:
		if address, ok := opcodeStackAddress(scope, 0); ok {
			r.recordReadAddress(address)
		}
	case vm.CALL, vm.CALLCODE, vm.DELEGATECALL, vm.STATICCALL:
		if address, ok := opcodeStackAddress(scope, 1); ok {
			r.recordReadAddress(address)
		}
	case vm.SELFDESTRUCT:
		r.recordReadAddress(scope.Address())
		r.recordWriteAddress(scope.Address())
		if address, ok := opcodeStackAddress(scope, 0); ok {
			r.recordWriteAddress(address)
		}
	}
}

// onBalanceChange 记录余额变化涉及的 Address。
// 普通转账和 gas 扣费都不一定进入 EVM，因此写集必须依赖 state hook 补齐。
func (r *blockLatencyRecorder) onBalanceChange(addr common.Address, _, _ *big.Int, _ tracing.BalanceChangeReason) {
	r.recordWriteAddress(addr)
}

// onNonceChangeV2 记录 nonce 变化涉及的 Address。
// tracing journal 不允许同时注册旧版 OnNonceChange 和新版 OnNonceChangeV2，所以这里只使用 V2。
func (r *blockLatencyRecorder) onNonceChangeV2(addr common.Address, _, _ uint64, _ tracing.NonceChangeReason) {
	r.recordWriteAddress(addr)
}

// onCodeChangeV2 记录代码变化涉及的 Address，例如合约创建或自毁。
// tracing journal 不允许同时注册旧版 OnCodeChange 和新版 OnCodeChangeV2，所以这里只使用 V2。
func (r *blockLatencyRecorder) onCodeChangeV2(addr common.Address, _ common.Hash, _ []byte, _ common.Hash, _ []byte, _ tracing.CodeChangeReason) {
	r.recordWriteAddress(addr)
}

// onStorageChange 记录 storage 写入涉及的合约 Address。
func (r *blockLatencyRecorder) onStorageChange(addr common.Address, _, _, _ common.Hash) {
	r.recordWriteAddress(addr)
}

// recordReadAddress 把 Address 加入当前交易读集。
func (r *blockLatencyRecorder) recordReadAddress(addr common.Address) {
	if r.currentTx == nil || r.currentReadSet == nil {
		return
	}
	r.currentReadSet[addr.Hex()] = struct{}{}
}

// recordWriteAddress 把 Address 加入当前交易写集。
func (r *blockLatencyRecorder) recordWriteAddress(addr common.Address) {
	if r.currentTx == nil || r.currentWriteSet == nil {
		return
	}
	r.currentWriteSet[addr.Hex()] = struct{}{}
}

// sortedAddressSet 将 Address set 转成稳定顺序的切片，保证 JSON 输出可复现。
func sortedAddressSet(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	addresses := make([]string, 0, len(set))
	for address := range set {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	return addresses
}

// opcodeStackAddress 从 opcode 执行前的栈中读取 Address 参数。
// nthFromTop=0 表示栈顶；CALL* 的目标地址在 gas 参数下面一层，因此使用 1。
func opcodeStackAddress(scope tracing.OpContext, nthFromTop int) (common.Address, bool) {
	stack := scope.StackData()
	index := len(stack) - 1 - nthFromTop
	if index < 0 {
		return common.Address{}, false
	}
	return common.Address(stack[index].Bytes20()), true
}

// normalizeInclusiveLatency 在持久化前从调用树重新计算 inclusive 耗时。
// 即使某个 frame 提前退出，存储结果也能保持一致。
func normalizeInclusiveLatency(trace *vm.ContractLatencyTrace) float64 {
	if trace == nil {
		return 0
	}
	total := trace.LatencyNS
	for _, child := range trace.Children {
		total += normalizeInclusiveLatency(child)
	}
	trace.InclusiveLatencyNS = total
	return total
}

// aggregateContractTrace 更新每个 frame 对应的全局运行平均值，包括嵌套合约 frame。
// 聚合 key 是 contractAddress_methodSelector；同一个合约函数每出现一次 frame 就计入一次 Count。
// 因此同一笔交易内多次调用、跨交易多次调用、嵌套调用里的同名函数调用，都会一起计算平均值。
func (r *blockLatencyRecorder) aggregateContractTrace(trace *vm.ContractLatencyTrace) {
	if trace == nil {
		return
	}
	aggregate := r.aggregates[trace.Key]
	if aggregate == nil {
		aggregate = &contractLatencyAggregate{
			Key:             trace.Key,
			ContractAddress: trace.ContractAddress,
			MethodSelector:  trace.MethodSelector,
		}
		r.aggregates[trace.Key] = aggregate
	}
	// 每个 frame 代表一次实际调用，平均值按调用次数而不是按交易次数计算。
	aggregate.Count++
	aggregate.TotalLatencyNS += trace.LatencyNS
	aggregate.AverageLatencyNS = aggregate.TotalLatencyNS / float64(aggregate.Count)
	aggregate.TotalInclusiveLatencyNS += trace.InclusiveLatencyNS
	aggregate.AverageInclusiveLatencyNS = aggregate.TotalInclusiveLatencyNS / float64(aggregate.Count)
	r.touched[trace.Key] = struct{}{}

	for _, child := range trace.Children {
		r.aggregateContractTrace(child)
	}
}

// writeBlockLatency 持久化一个区块的 latency 输出。
//
// key 布局：
//
//	<blockNumber>                         -> 区块 JSON，value.txs 中每笔交易包含 txid/txhash/latency/rw
//	<contractAddress>_<methodSelector>    -> 合约方法平均 latency JSON
func writeBlockLatency(db *pebble.DB, blockNumber uint64, records []*txLatencyRecord, aggregates map[string]*contractLatencyAggregate, touched map[string]struct{}) error {
	batch := db.NewBatch()
	batchSize := 0
	defer batch.Close()

	commitBatch := func() error {
		if batchSize == 0 {
			return nil
		}
		// 使用 pebble.Sync 提交 batch，保证当前区块的 latency 结果落到 WAL。
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
		if err := batch.Close(); err != nil {
			return err
		}
		batch = db.NewBatch()
		batchSize = 0
		return nil
	}

	put := func(key string, value []byte) error {
		if err := batch.Set([]byte(key), value, nil); err != nil {
			return err
		}
		batchSize += len(key) + len(value)
		if batchSize >= replayLatencyBatchSize {
			if err := commitBatch(); err != nil {
				return err
			}
		}
		return nil
	}

	// 交易主表只按区块写一次：一个区块一个 key，value 里保存这个区块的全部交易。
	blockData, err := json.Marshal(blockLatencyValue{
		BlockNumber: blockNumber,
		Txs:         records,
	})
	if err != nil {
		return err
	}
	if err := put(latencyBlockKey(blockNumber), blockData); err != nil {
		return err
	}

	for key := range touched {
		// 只重写本区块触达过的 aggregate，控制写入量。
		aggregate := aggregates[key]
		if aggregate == nil {
			continue
		}
		data, err := json.Marshal(aggregate)
		if err != nil {
			return err
		}
		// 合约平均值直接使用 contractAddress_methodSelector 作为 Pebble key。
		if err := put(contractLatencyKey(aggregate.ContractAddress, aggregate.MethodSelector), data); err != nil {
			return err
		}
	}
	return commitBatch()
}

// latencyBlockKey 生成区块交易主表 key。
// key 直接使用十进制区块号；这样交易主表就是 blockNumber -> txs(...)。
func latencyBlockKey(blockNumber uint64) string {
	return fmt.Sprintf("%d", blockNumber)
}

// Reference 维护 trie root 引用并在内存超过阈值时触发 Cap。
// 这个大内存 replay 路径按区块推进状态，及时解除旧 root 引用可以控制内存占用。
func Reference(alldb *database.AllDBForState, parentRoot, root common.Hash) {
	// 引用当前root
	alldb.TrieDB.Reference(root, common.Hash{})

	// 立即解除上一个root的引用
	alldb.TrieDB.Dereference(parentRoot)

	// 检查并清理内存
	_, nodes, imgs := alldb.TrieDB.Size()
	limit := common.StorageSize(TrieMemoryLimit)
	imgLimit := common.StorageSize(ImageMemoryLimit)

	if nodes > limit || imgs > imgLimit {
		fmt.Printf("[Memory Warning] Nodes: %v/%v, Images: %v/%v\n",
			common.StorageSize(nodes), limit,
			common.StorageSize(imgs), imgLimit)
		alldb.TrieDB.Cap(limit - replayTrieCapReserve)
	}
}

// ReplayWithRecordOpCodeTiming 是带内存管理的交易重放入口。
// 第一遍执行推进真实状态，第二遍在 StateDB 副本上采集交易和合约 latency，并按区块写入 Pebble。
func ReplayWithRecordOpCodeTiming() {
	processor, frdb, err := newProcessor()
	if err != nil {
		panic(err)
		return
	}
	defer frdb.Close()

	latencyDB, latencyDBPath, err := newReplayLatencyDB()
	if err != nil {
		panic(err)
		return
	}
	defer latencyDB.Close()
	// 串行 replay 只使用 worker 0，所以只初始化 1 个 worker 槽位。
	// VM 侧 OpenTxLatencyTrace 不负责扩容，避免影响其他模块。
	vm.InitTxLatencyTrace(1)

	blockPre, err := database.GetBlockByNumber(frdb, replay_config.RootBlockNumber)
	if err != nil {
		panic(err)
		return
	}

	var parentStateRoot = blockPre.Root()
	alldbForState, err := database.NewAllDBForState(
		database.DefaultStateDBConfig,
		blockPre.Number(),
		blockPre.Root(),
		false,
		false,
	)
	if err != nil {
		panic(err)
	}
	defer alldbForState.Close()
	// contractAggregates 跨区块保留，因此 contractAddress_methodSelector 表示整个 replay 区间的平均值。
	contractAggregates := make(map[string]*contractLatencyAggregate)

	fmt.Printf("\n")
	fmt.Printf("╔═══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  Replay with Aggressive Memory Management (128GB Config)\n")
	fmt.Printf("╠═══════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Memory Limits:\n")
	fmt.Printf("║    • Trie:   %v\n", common.StorageSize(TrieMemoryLimit))
	fmt.Printf("║    • Images: %v\n", common.StorageSize(ImageMemoryLimit))
	fmt.Printf("║  Strategy:\n")
	fmt.Printf("║    • Commit every %d blocks (真正释放内存)\n", CommitInterval)
	fmt.Printf("║    • Cap every %d blocks (整理内存)\n", CapInterval)
	fmt.Printf("║    • GC every %d blocks (Go垃圾回收)\n", ForceGCInterval)
	fmt.Printf("║  Latency DB: %s\n", latencyDBPath)
	fmt.Printf("╚═══════════════════════════════════════════════════════════╝\n\n")

	processStartTime := time.Now()
	blockCount := uint64(0)

	// 设置更激进的GC参数
	debug.SetGCPercent(50) // GC触发阈值降低到50%（默认100%）

	for blockNumber := replay_config.StartBlockNumber; blockNumber.Cmp(replay_config.FinishBlockNumber) == -1; blockNumber = blockNumber.Add(blockNumber, replay_config.AddSpan) {

		blockCount++

		// 更新StateDB
		err := alldbForState.UpdateStateDB(parentStateRoot)
		if err != nil {
			panic(err)
			return
		}

		block, err := database.GetBlockByNumber(frdb, blockNumber)
		if err != nil {
			panic(err)
			return
		}

		// 复制 StateDB，第二遍 latency 统计只在这份副本上执行。
		statedbCopy := alldbForState.StateDB.Copy()

		// 第一遍执行推进真实 replay 状态，后续会提交并用于 state root 校验。
		_, err = processor.Process(block, alldbForState.StateDB, config.DefaultVmConfig)
		if err != nil {
			fmt.Println("First process error:", err)
		}

		// 第二遍执行跑在 StateDB 副本上，并开启 latency tracing。
		// 这样测量写入和 tracer 开销不会影响用于提交、对比 root 的状态实例。
		latencyRecorder := newBlockLatencyRecorder(block.NumberU64(), contractAggregates)
		timingConfig := config.DefaultVmConfig
		timingConfig.Tracer = latencyRecorder.hooks()
		_, err = processor.Process(block, statedbCopy, timingConfig)
		if err != nil {
			fmt.Println("Second process error:", err)
		}
		// 持久化本区块全部交易记录，以及本区块触达过的合约平均值。
		if err := writeBlockLatency(latencyDB, block.NumberU64(), latencyRecorder.records, contractAggregates, latencyRecorder.touched); err != nil {
			fmt.Println("Latency DB write error:", err)
		}

		// 提交状态
		root, _, err := alldbForState.StateDB.CommitWithUpdate(
			block.NumberU64(),
			config.MainnetChainConfig.IsEIP158(block.Number()),
			config.MainnetChainConfig.IsCancun(block.Number(), block.Time()),
		)
		if err != nil {
			fmt.Println("Commit error:", err)
		}

		// 验证
		if root != block.Root() {
			fmt.Printf("⚠️  Root mismatch at block %v: expected %v, got %v\n",
				blockNumber, block.Root(), root)
		}

		// Copy用完立即置nil
		statedbCopy = nil

		// Reference/Dereference管理
		Reference(alldbForState, parentStateRoot, root)
		parentStateRoot = root

		// 每N个区块Cap内存
		if blockCount%CapInterval == 0 {
			_, nodes, imgs := alldbForState.TrieDB.Size()
			fmt.Printf("\n[Cap] Block %d - Before: Nodes=%v, Images=%v\n",
				blockNumber, common.StorageSize(nodes), common.StorageSize(imgs))

			alldbForState.TrieDB.Cap(common.StorageSize(TrieMemoryLimit) - replayTrieCapReserve)

			_, nodesAfter, imgsAfter := alldbForState.TrieDB.Size()
			fmt.Printf("[Cap] After: Nodes=%v, Images=%v (Freed: %v)\n",
				common.StorageSize(nodesAfter), common.StorageSize(imgsAfter),
				common.StorageSize((nodes+imgs)-(nodesAfter+imgsAfter)))
		}

		// 每N个区块完全Commit（关键！这个才能真正释放内存）
		if blockCount%CommitInterval == 0 {
			_, nodes, imgs := alldbForState.TrieDB.Size()
			fmt.Printf("\n[Commit] Block %d - Before: Nodes=%v, Images=%v\n",
				blockNumber, common.StorageSize(nodes), common.StorageSize(imgs))
			fmt.Printf("[Commit] Performing full commit to disk...\n")

			commitStart := time.Now()
			// Commit(root, true) - true表示同时清理缓存
			err = alldbForState.TrieDB.Commit(root, true)
			if err != nil {
				fmt.Printf("⚠️  Commit error: %v\n", err)
			}
			commitDuration := time.Since(commitStart)

			_, nodesAfter, imgsAfter := alldbForState.TrieDB.Size()
			fmt.Printf("[Commit] After: Nodes=%v, Images=%v (Freed: %v)\n",
				common.StorageSize(nodesAfter), common.StorageSize(imgsAfter),
				common.StorageSize((nodes+imgs)-(nodesAfter+imgsAfter)))
			fmt.Printf("[Commit] Duration: %v\n", commitDuration)
		}

		// 每N个区块强制GC
		if blockCount%ForceGCInterval == 0 {
			var m1, m2 runtime.MemStats
			runtime.ReadMemStats(&m1)

			fmt.Printf("\n[Go GC] Block %d - Before: Alloc=%v, Sys=%v\n",
				blockNumber, common.StorageSize(m1.Alloc), common.StorageSize(m1.Sys))

			runtime.GC()
			debug.FreeOSMemory() // 强制归还内存给OS

			runtime.ReadMemStats(&m2)
			fmt.Printf("[Go GC] After: Alloc=%v, Sys=%v (Freed: %v)\n",
				common.StorageSize(m2.Alloc), common.StorageSize(m2.Sys),
				common.StorageSize(m1.Alloc-m2.Alloc))
		}

		// 定期检查内存
		if blockCount%MemCheckInterval == 0 {
			_, nodes, imgs := alldbForState.TrieDB.Size()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			if blockCount%100 == 0 {
				// 详细报告
				fmt.Printf("\n")
				fmt.Printf("═══════════════════════════════════════════\n")
				fmt.Printf(" Block %d Progress Report\n", blockNumber)
				fmt.Printf("═══════════════════════════════════════════\n")
				fmt.Printf("Trie DB:\n")
				fmt.Printf("  Nodes:  %v / %v (%.1f%%)\n",
					common.StorageSize(nodes), common.StorageSize(TrieMemoryLimit),
					float64(nodes)/float64(TrieMemoryLimit)*100)
				fmt.Printf("  Images: %v / %v (%.1f%%)\n",
					common.StorageSize(imgs), common.StorageSize(ImageMemoryLimit),
					float64(imgs)/float64(ImageMemoryLimit)*100)
				fmt.Printf("Go Runtime:\n")
				fmt.Printf("  Alloc:  %v\n", common.StorageSize(m.Alloc))
				fmt.Printf("  Sys:    %v\n", common.StorageSize(m.Sys))
				fmt.Printf("  NumGC:  %d\n", m.NumGC)
				fmt.Printf("Performance:\n")
				fmt.Printf("  Blocks: %d\n", blockCount)
				fmt.Printf("  Avg:    %v/block\n", time.Since(processStartTime)/time.Duration(blockCount))
				fmt.Printf("═══════════════════════════════════════════\n\n")
			} else {
				// 简单进度
				fmt.Printf("[Progress] Block %d | Trie: %v | Go: %v | Avg: %v\n",
					blockNumber, common.StorageSize(nodes+imgs), common.StorageSize(m.Alloc),
					time.Since(processStartTime)/time.Duration(blockCount))
			}
		}

	}

	totalTime := time.Since(processStartTime)
	fmt.Printf("\n")
	fmt.Printf("╔═══════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  ✓ Replay Completed\n")
	fmt.Printf("╠═══════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Blocks:    %d\n", blockCount)
	fmt.Printf("║  Duration:  %v\n", totalTime)
	fmt.Printf("║  Avg:       %v/block\n", totalTime/time.Duration(blockCount))
	fmt.Printf("║  Contracts: %d averaged entries\n", len(contractAggregates))
	fmt.Printf("╚═══════════════════════════════════════════════════════════╝\n")
}
