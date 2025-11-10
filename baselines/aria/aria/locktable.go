package aria

import (
	optmeCommon "Janus/plugin/Common"
)

// AriaLockEntry 表示在悲观回退阶段的锁依赖关系
type AriaLockEntry struct {
	DepsGet []*AriaTransaction // 记录读依赖的事务
	DepsPut []*AriaTransaction // 记录写依赖的事务
}

// AriaLockTable 封装 Common.Table，用于 fallback 阶段记录事务依赖
type AriaLockTable struct {
	*optmeCommon.Table[*AriaLockEntry]
}

// NewAriaLockTable 创建 AriaLockTable
func NewAriaLockTable(partitions int) *AriaLockTable {
	return &AriaLockTable{
		Table: optmeCommon.NewTable[*AriaLockEntry](partitions),
	}
}

// AddGetDep 在 key 的 DepsGet 列表中添加依赖事务
func (t *AriaLockTable) AddGetDep(key string, tx *AriaTransaction) {
	t.Table.PutWithDefault(key, &AriaLockEntry{}, func(e **AriaLockEntry) {
		entry := *e
		entry.DepsGet = append(entry.DepsGet, tx)
	})
}

// AddPutDep 在 key 的 DepsPut 列表中添加依赖事务
func (t *AriaLockTable) AddPutDep(key string, tx *AriaTransaction) {
	t.Table.PutWithDefault(key, &AriaLockEntry{}, func(e **AriaLockEntry) {
		entry := *e
		entry.DepsPut = append(entry.DepsPut, tx)
	})
}

// GetEntry 返回指定 key 的锁依赖信息（若不存在则返回空结构）
func (t *AriaLockTable) GetEntry(key string) *AriaLockEntry {
	var result *AriaLockEntry
	t.Table.Get(key, func(e *AriaLockEntry) {
		result = e
	})
	if result == nil {
		result = &AriaLockEntry{}
	}
	return result
}

// ClearDeps 清空某个 key 上的依赖
func (t *AriaLockTable) ClearDeps(key string) {
	t.Table.PutWithDefault(key, &AriaLockEntry{}, func(e **AriaLockEntry) {
		entry := *e
		entry.DepsGet = nil
		entry.DepsPut = nil
	})
}
