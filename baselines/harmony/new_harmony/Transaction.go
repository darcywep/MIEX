package newHarmony

import (
	"Janus/baselines/common"
	"Janus/ethereum/core/types"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// HarmonyTransaction harmony transaction with local read and write set.
type HarmonyTransaction struct {
	Tx           *common.BasicTransaction
	EthTx        *types.Transaction
	BlockID      int
	originalTxID int
	ID           uint32
	BatchID      uint32
	FlagConflict bool
	Committed    atomic.Bool
	StartTime    time.Time
	LocalGet     map[string]string
	LocalPut     map[string]string

	// ========== 修改点 1: MinOut/MaxIn 类型从 uint32 改为 int64 ==========
	// 原因: 论文 Algorithm 1 Line 3 要求 Tj.max_in ← -inf。
	// uint32 类型无法表示负无穷。原代码用 MaxIn=0 代替 -inf，
	// 但当 TxID 从 0 或 1 开始时，条件 min_out <= max_in 可能误判:
	//
	//   例: Tj(ID=5) 只有一条出边到 Ti(ID=0)，无入边
	//   原代码: MinOut=0, MaxIn=0 → 0<5 && 0<=0 → true → 误 abort
	//   修复后: MinOut=0, MaxIn=MinInt64 → 0<5 && 0<=MinInt64 → false → 正确
	//
	// TxID 的生成逻辑在 TxGenerator.go:
	//   txid := blockID*blockSize + i + 1 (从 1 开始)
	// 因此当前 MaxIn=0 在 MinOut=1 时不会触发 (1<=0 为 false)，
	// 但如果 blockID=0 且 blockSize 变化导致 txid=0，就会出 bug。
	// 使用 int64 + MinInt64 是严谨的修复。
	MinOut int64
	MaxIn  int64

	OutBatchID uint32
	InBatchID  uint32

	// ========== 修改点 2: 新增 mu 保护 MinOut/MaxIn 的并发更新 ==========
	// 原因: OnSeeingRWDependency 在 Table.Put 的回调内被调用，
	// 回调受该 key 所在分区的 SpinLock 保护。但问题在于:
	//
	//   worker1 处理 key A (分区 0): 更新 Tj.MinOut
	//   worker2 处理 key B (分区 1): 同时更新 Tj.MinOut
	//
	// key A 和 key B 在不同分区，持有不同的 SpinLock，
	// 因此对 Tj.MinOut 的并发写没有保护 → data race。
	//
	// 类似地, MinOut 和 OutBatchID 的更新不是原子的:
	//   if Ti.ID < Tj.MinOut {
	//       Tj.MinOut = Ti.ID        // ← 赋值1
	//       Tj.OutBatchID = Ti.BatchID // ← 赋值2
	//   }
	// 如果另一个 goroutine 在赋值1和赋值2之间读取，
	// 会看到 MinOut 来自事务 A 但 OutBatchID 来自事务 B。
	mu sync.Mutex

	// ========== 修改点 3: 新增 UpdatedKeys 用于 Update Reordering ==========
	// 原因: 论文 Algorithm 2 Line 5:
	//   T_current.updated_keys.append(key)
	// 每个事务需要记录它更新了哪些 key，commit 阶段遍历这些 key
	// 进行 reordering 和 coalescence。原代码完全缺少此字段。
	UpdatedKeys []string
}

// NewHarmonyTransaction 构造函数
func NewHarmonyTransaction(inner *common.BasicTransaction, id uint32, batchID uint32, blockID, originalTxID int) *HarmonyTransaction {
	return &HarmonyTransaction{
		Tx:           inner,
		ID:           id,
		BatchID:      batchID,
		BlockID:      blockID,
		originalTxID: originalTxID,
		// ========== 修改点 1 (续): 初始化值修改 ==========
		// 论文 Algorithm 1 Line 2: Tj.min_out ← j + 1
		// 论文 Algorithm 1 Line 3: Tj.max_in  ← -inf
		//
		// 原代码: MinOut: id + 1 (uint32), MaxIn: 0 (uint32)
		// 修改后: MinOut: int64(id) + 1, MaxIn: math.MinInt64
		MinOut:     int64(id) + 1,
		MaxIn:      math.MinInt64,
		OutBatchID: batchID,
		InBatchID:  batchID,
		StartTime:  time.Now(),
		LocalGet:   make(map[string]string),
		LocalPut:   make(map[string]string),
	}
}

func (executor *HarmonyExecutor) GetStorage_From_Table(tx *HarmonyTransaction, readSet map[string]bool) {
	var keys strings.Builder
	for key := range readSet {
		keys.WriteString(key + " ")
		var value string
		executor.table.table.Get(key, func(entry HarmonyEntry) {
			value = entry.Value
		})
		tx.LocalGet[key] = value
	}
}

func (executor *HarmonyExecutor) SetStorage_Into_Table(tx *HarmonyTransaction, writeSet map[string]bool, value string) {
	var keys strings.Builder
	for key := range writeSet {
		keys.WriteString(key + " ")
		executor.table.table.Put(key, func(entry *HarmonyEntry) {
			(*entry).Value = value
		})
	}
}
