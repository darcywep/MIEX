package aria

import (
	"sync"
	"sync/atomic"
)

// -----------------------------
// AriaTable: 存储事务的表
// -----------------------------
type shard struct {
	mu sync.RWMutex
	m  map[string]*AriaEntry
}

type AriaTable struct {
	shards []*shard
	n      int
}

func NewAriaTable(partitions int) *AriaTable {
	if partitions <= 0 {
		partitions = 1
	}
	shards := make([]*shard, partitions)
	for i := 0; i < partitions; i++ {
		shards[i] = &shard{m: make(map[string]*AriaEntry)}
	}
	return &AriaTable{shards: shards, n: partitions}
}

func (t *AriaTable) shardFor(key string) *shard {
	idx := int((uint64(len(key)) + 0x9e3779b97f4a7c15) % uint64(t.n))
	return t.shards[idx]
}

func (t *AriaTable) Put(key string, mutator func(*AriaEntry)) {
	s := t.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	ent, ok := s.m[key]
	if !ok {
		ent = &AriaEntry{}
		s.m[key] = ent
	}
	mutator(ent)
}

func (t *AriaTable) Get(key string, reader func(AriaEntry)) {
	s := t.shardFor(key)
	s.mu.RLock()
	ent, ok := s.m[key]
	var copyEntry AriaEntry
	if ok {
		copyEntry = *ent
	}
	s.mu.RUnlock()
	reader(copyEntry)
}
