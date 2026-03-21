package newHarmony

import (
	"Janus/baselines/common"
	"math"
	"sort"
)

// HarmonyEntry harmony table entry for first round execution
type HarmonyEntry struct {
	Value          string
	ReservedGetTxs []*HarmonyTransaction
	ReservedPutTxs []*HarmonyTransaction

	// ========== 修改点 4: 新增 UpdateCmds 和 Handled ==========
	// 原因: 论文 Algorithm 2 使用 update_reservation 哈希表将
	// key 映射到该 key 上所有事务的 update command 列表。
	//
	// 论文 Algorithm 2 Line 3-4:
	//   update_cmds ← update_reservation.search(key)
	//   update_cmds.append(update_command)
	//
	// 论文 Algorithm 2 Line 11-12:
	//   if update_cmds.handled == False then
	//     update_cmds.handled ← True
	//
	// 这确保每个 key 的 reordering + coalescence 只由第一个到达的事务执行，
	// 其他事务看到 handled=true 直接跳过，从而实现并行。
	// 原代码完全缺少这两个字段。
	UpdateCmds []*UpdateCommand
	Handled    bool
}

// ========== 修改点 5: 新增 UpdateCommand 结构体 ==========
// 原因: 论文 Section 3.3 中 update command 是一个包含操作语义的对象
// (如 add(x,10), mul(x,3))。在当前实现中简化为事务引用+值。
// 用于 commit 阶段的 reordering (按 min_out 排序) 和
// coalescence (合并同一 key 的多个更新)。
type UpdateCommand struct {
	Tx    *HarmonyTransaction
	Key   string
	Value string
}

// HarmonyTable harmony table for first round execution
type HarmonyTable struct {
	table *common.Table[HarmonyEntry]
}

func NewHarmonyTable(partitions int) *HarmonyTable {
	return &HarmonyTable{
		table: common.NewTable[HarmonyEntry](partitions),
	}
}

// OnSeeingRWDependency handle a r-w dependency
// 论文 Algorithm 1 Line 6-9:
//
//	Event on_seeing_rw_dependency(Ti ←rw— Tj):
//	  Tj.min_out ← min(i, Tj.min_out)
//	  Ti.max_in  ← max(j, Ti.max_in)
//
// Ti: 出边目标事务 (TID 较小的一方)
// Tj: 出边来源事务 (TID 较大的一方)
//
// ========== 修改点 6: 使用事务级 mutex 保护并发更新 ==========
// 原因详述:
//
// 调用链分析:
//
//	ExecutorGetStorage → Table.Put(key, callback) → callback 中遍历 ReservedPutTxs → OnSeeingRWDependency
//	ExecutorSetStorage → Table.Put(key, callback) → callback 中遍历 ReservedGetTxs → OnSeeingRWDependency
//
// Table.Put 持有该 key 所在分区的 SpinLock。但考虑以下场景:
//
//	事务 Tj 写了 key_A (分区0) 和 key_B (分区1)
//	事务 T1 (worker1) 读 key_A → 持有分区0锁 → 调用 OnSeeingRWDependency 更新 Tj.MinOut
//	事务 T2 (worker2) 读 key_B → 持有分区1锁 → 同时调用 OnSeeingRWDependency 更新 Tj.MinOut
//
// 分区0和分区1的锁不同，因此两个 worker 同时写 Tj.MinOut → data race
//
// 原代码没有任何保护:
//
//	if Ti.ID < Tj.MinOut {
//	    Tj.MinOut = Ti.ID
//	    Tj.OutBatchID = Ti.BatchID
//	}
//
// 这里 read Tj.MinOut → compare → write Tj.MinOut → write Tj.OutBatchID
// 不是原子操作，并发时可能:
//
//	(a) 丢失更新: 较小的 MinOut 被较大值覆盖
//	(b) 不一致: MinOut 来自一个事务但 OutBatchID 来自另一个事务
func (ht *HarmonyTable) OnSeeingRWDependency(Ti *HarmonyTransaction, Tj *HarmonyTransaction) {
	// 按 ID 排序获取锁，避免死锁
	// 固定锁序: 先锁 ID 小的，再锁 ID 大的
	first, second := Ti, Tj
	if Ti.ID > Tj.ID {
		first, second = Tj, Ti
	}

	first.mu.Lock()
	second.mu.Lock()

	tiID := int64(Ti.ID)
	if tiID < Tj.MinOut {
		Tj.MinOut = tiID
		Tj.OutBatchID = Ti.BatchID
	}

	tjID := int64(Tj.ID)
	if tjID > Ti.MaxIn {
		Ti.MaxIn = tjID
		Ti.InBatchID = Tj.BatchID
	}

	second.mu.Unlock()
	first.mu.Unlock()
}

// ========== 修改点 7: 新增 ApplyWriteSets ==========
// 原因: 这是论文的核心提交逻辑 — Algorithm 2 Line 8-17。
//
// 原代码的 Commit 函数:
//
//	for key, value := range tx.LocalPut {
//	    executor.table.table.Put(key, func(entry *HarmonyEntry) {
//	        (*entry).Value = value
//	    })
//	}
//
// 问题: 每个事务独立地将自己的 LocalPut 写入 table，
// 写入顺序取决于哪个 worker 先到达 → 不确定性。
//
// 论文要求:
//
//	(1) 对同一 key 的所有未 abort 事务的 update commands,
//	    按 min_out 升序排序 (Rule 2: Reordering Rule)
//	(2) 按排序后的顺序依次应用 (或合并后一次应用: Coalescence)
//	(3) 只由第一个到达该 key 的事务执行 (handled 标志)
//
// 缺少 Reordering 的后果:
//
//	论文 Theorem 1 证明: "给定无环 rw-subgraph，如果 update commands
//	按 rw-subgraph 的拓扑序 reorder，则完整依赖图也无环"
//	→ 没有 reordering，ww/wr-dependency 方向可能与 rw-dependency 矛盾
//	→ 完整依赖图可能有环 → 可序列化性被破坏
//
// 缺少 Coalescence 的后果:
//
//	论文 Section 3.3.2 Figure 5: 不做 coalescence 时，
//	T1 的 add(x,10) 和 T2 的 mul(x,3) 需要分别执行两次完整的
//	index lookup → lock page → update → unlock 流程。
//	Coalescence 合并为 mul(add(x,10),3) 只执行一次 → 性能差异
func (ht *HarmonyTable) ApplyWriteSets(tx *HarmonyTransaction) {
	for _, key := range tx.UpdatedKeys {
		ht.table.Put(key, func(entry *HarmonyEntry) {
			// 论文 Algorithm 2 Line 11-12:
			// if update_cmds.handled == False then
			//   update_cmds.handled ← True
			// 确保同一 key 只被处理一次。
			// 多个事务可能更新同一 key，但只有第一个到达的事务
			// 负责排序和应用所有 commands，其他事务跳过。
			// 这样其他事务可以并行处理自己的其他 key。
			if entry.Handled {
				return
			}
			entry.Handled = true

			// 论文 Algorithm 2 Line 13:
			// update_cmds.filter(update_command.T.aborted == False)
			activeCmds := make([]*UpdateCommand, 0, len(entry.UpdateCmds))
			for _, cmd := range entry.UpdateCmds {
				if !cmd.Tx.FlagConflict { // FlagConflict == true 表示已 abort
					activeCmds = append(activeCmds, cmd)
				}
			}

			if len(activeCmds) == 0 {
				return
			}

			// 论文 Algorithm 2 Line 14:
			// update_cmds.sort_by(update_command.T.min_out)
			//
			// 论文 Rule 2 (Reordering Rule):
			// "reorder the transactions that update the same record
			//  by the ascending order of their minimal outgoing TIDs
			//  (i.e., min_out), and break the tie by their own TIDs"
			sort.Slice(activeCmds, func(i, j int) bool {
				if activeCmds[i].Tx.MinOut != activeCmds[j].Tx.MinOut {
					return activeCmds[i].Tx.MinOut < activeCmds[j].Tx.MinOut
				}
				return activeCmds[i].Tx.ID < activeCmds[j].Tx.ID
			})

			// 论文 Algorithm 2 Line 15-16:
			// coalesced_update ← coalesce(update_cmds)
			// apply(coalesced_update)
			//
			// 当前实现中写入的是最终值 (而非 add/mul 等 update command)，
			// 所以 coalescence 简化为: 按排序后的顺序依次覆盖，
			// 最终值就是排序中最后一个事务的写入值。
			//
			// 注意: 如果未来支持真正的 update command (如 add(x,10)),
			// 这里需要改为: 按顺序依次求值, 前一个的输出作为后一个的输入。
			// 例: add(x,10) 然后 mul(x,3) → x = (x+10)*3
			finalValue := ""
			for _, cmd := range activeCmds {
				finalValue = cmd.Value
			}

			entry.Value = finalValue
		})
	}
}

// ========== 修改点 8: 新增 ResetEntry ==========
// 原因: 每个 block 处理完成后，HarmonyEntry 中残留着该 block 的:
//   - ReservedGetTxs: 读过该 key 的事务列表
//   - ReservedPutTxs: 写过该 key 的事务列表
//   - UpdateCmds:     该 key 的 update command 列表
//   - Handled:        是否已被某事务处理
//
// 如果不清理，下一个 block 的事务在模拟阶段会看到上一个 block 的
// ReservedPutTxs/ReservedGetTxs，导致:
//
//	(a) 建立错误的跨 block rw-dependency
//	(b) MinOut/MaxIn 被错误更新
//	(c) ApplyWriteSets 发现 Handled=true 直接跳过，不执行 reordering
func (ht *HarmonyTable) ResetEntry(key string) {
	ht.table.Put(key, func(entry *HarmonyEntry) {
		entry.ReservedGetTxs = nil
		entry.ReservedPutTxs = nil
		entry.UpdateCmds = nil
		entry.Handled = false
	})
}

// HarmonyLockEntry harmony table entry for fallback pessimistic execution
type HarmonyLockEntry struct {
	DepsGet []*HarmonyTransaction
	DepsPut []*HarmonyTransaction
}

// NewHarmonyLockEntry 构造函数
func NewHarmonyLockEntry() *HarmonyLockEntry {
	return &HarmonyLockEntry{
		DepsGet: make([]*HarmonyTransaction, 0),
		DepsPut: make([]*HarmonyTransaction, 0),
	}
}

type HarmonyLockTable struct {
	table *common.Table[HarmonyLockEntry]
}

func NewHarmonyLockTable(partitions int) *HarmonyLockTable {
	return &HarmonyLockTable{
		common.NewTable[HarmonyLockEntry](partitions),
	}
}

// ========== 以下为辅助常量, 配合 Verify 中的类型比较 ==========
const MaxInInitValue = math.MinInt64
