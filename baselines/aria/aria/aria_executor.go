package aria

import (
	optmeCommon "Janus/baselines/common"
	lvm "Janus/core/evm"
	"Janus/tools"
	"time"
)

// AriaExecutor 的职责变成“处理单次收到的 tx”：
// 1) Execute(tx)（如果 tx == nil 则跳过）
// 2) Reserve(tx)（如果 tx == nil 则跳过）
// 3) barrier.Wait() （等待所有 worker 完成本列 Execute+Reserve）
// 4) Verify/Commit （或判断 abort）
// 5) 将 abort（或 nil）发送回 controller（通过 controller 提供的 channels）
type AriaExecutor struct {
	aria          *Aria
	levm          *lvm.LEVM
	statistics    *optmeCommon.Statistics
	table         *AriaTable
	enableReorder bool
	workerID      int
}

// NewAriaExecutor 创建单个 worker 的 Executor
func NewAriaExecutor(a *Aria, levm *lvm.LEVM, workerID int) *AriaExecutor {
	return &AriaExecutor{
		aria:          a,
		levm:          levm,
		statistics:    a.statistics,
		table:         a.table,
		enableReorder: a.enableReorder,
		workerID:      workerID,
	}
}

// ProcessOneTx 由 worker 的 goroutine 调用：处理 controller 发来的单笔交易（可能为 nil）
// 它会在函数内执行 Execute/Reserve -> barrier.Wait() -> Verify/Commit，然后把结果写入 aria.resultChans[workerID]
func (e *AriaExecutor) ProcessOneTx(tx *AriaTransaction) {
	workerID := e.workerID

	// Stage A: Execute + Reserve (本地)
	if tx != nil {
		tx.StartTime = time.Now()
		// 本地执行（基于 snapshot）；不要改写共享表
		e.Execute(tx)
		// 只为写集合做 reservation（论文语义）
		e.Reserve(tx)
		e.statistics.JournalExecute()
	}

	// Stage B: barrier 等待所有 worker 完成自己的 Execute+Reserve（这是“列同步”）
	e.aria.barrier.Wait()

	// Stage C: Verify + Commit （并发地在各 worker 上进行）
	var war, waw, raw bool
	if tx != nil {
		// 检查依赖（RAW/WAW/WAR）
		for key := range tx.LocalGet {
			if !e.table.CompareReservedPut(tx, key) {
				raw = true
			}
		}
		for key := range tx.LocalPut {
			if !e.table.CompareReservedPut(tx, key) {
				waw = true
			}
			if !e.table.CompareReservedGet(tx, key) {
				war = true
			}
		}
		//fmt.Println("war:", war, "waw:", waw, "raw:", raw, "tx:", tx.ID, "worker:", workerID)

		var conflict bool
		if e.enableReorder {
			conflict = waw || (raw && war)
		} else {
			conflict = waw || raw
		}

		if conflict {
			// 标记中止并把 tx 返回给 controller，用于下一个 block 的重试（controller 会把它插到下一区块的开头）
			//fmt.Println("transaction aborted:", tx.ID, "worker:", workerID, "war:", war, "waw:", waw, "raw:", raw)
			tx.SetConflict(true)
			// 通过 channel 返回 abort 给 controller
			e.aria.resultChans[workerID] <- tx
			if tools.TraceAbort {
				recordAriaAbort(tx)
			}
			e.statistics.AddRollbackCount()
			return
		}

		// 否则提交
		e.Commit(tx)
		e.statistics.JournalCommit(uint32(time.Since(tx.StartTime).Microseconds()))
		// 返回 nil 表示未 abort
		e.aria.resultChans[workerID] <- nil
		return
	}

	// tx == nil 的情况：没有事务，仍需在 resultChans 发送 nil（占位），以便 controller 收到每个 worker 的响应
	e.aria.resultChans[workerID] <- nil
}

// Execute: 填充 LocalGet/LocalPut 并调用 tx.Execute（不要写全局表）
func (e *AriaExecutor) Execute(tx *AriaTransaction) {
	// 读写集由 LatencyDB 模拟信息或原 SmallBank 规则统一填充，避免真实负载再依赖 TxType 推导地址。
	tools.FillStringReadWriteSet(tx.Inner.EthTx, tx.Inner.Vertex.ReadKeys, tx.Inner.Vertex.WriteKeys)
	tools.FillStringReadWriteSet(tx.Inner.EthTx, tx.LocalGet, tx.LocalPut)
	tx.Execute(e.levm, e.workerID)
}

// Reserve: 对读写集合做预约，用于后续 RAW/WAW/WAR 检测。
func (e *AriaExecutor) Reserve(tx *AriaTransaction) {
	for key := range tx.LocalGet {
		e.table.ReserveGet(tx, key)
	}
	for key := range tx.LocalPut {
		e.table.ReservePut(tx, key)
	}
}

// Commit: 把 tx.LocalPut 安装到全局表（Install）
func (e *AriaExecutor) Commit(tx *AriaTransaction) {
	for key, value := range tx.LocalPut {
		e.table.Table.Put(key, func(entry *AriaEntry) {
			(*entry).Value = value
		})
	}
	tx.SetCommitted(true)
}
