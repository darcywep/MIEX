package utils

import (
	"container/heap"
	"log"
	"sync/atomic"
)

// ---------------- SpinLock ----------------

// SpinLock 是轻量级自旋锁实现
type SpinLock struct {
	flag int32
}

func (s *SpinLock) Lock() {
	for !atomic.CompareAndSwapInt32(&s.flag, 0, 1) {
	}
}

func (s *SpinLock) Unlock() {
	atomic.StoreInt32(&s.flag, 0)
}

// ---------------- Guard ----------------

// Guard 实现类似 C++ 的作用域锁（RAII）
// 在构造时上锁，销毁时解锁
type Guard struct {
	lock *SpinLock
}

func NewGuard(l *SpinLock) *Guard {
	l.Lock()
	return &Guard{lock: l}
}

func (g *Guard) Done() {
	g.lock.Unlock()
}

// ---------------- LockQueue ----------------

type LockQueue[T any] struct {
	mu    SpinLock
	queue []*T
}

func (lq *LockQueue[T]) Pop() *T {
	g := NewGuard(&lq.mu)
	defer g.Done()

	if len(lq.queue) == 0 {
		return nil
	}
	tx := lq.queue[0]
	lq.queue = lq.queue[1:]
	return tx
}

func (lq *LockQueue[T]) Push(tx *T) {
	g := NewGuard(&lq.mu)
	defer g.Done()
	lq.queue = append(lq.queue, tx)
}

func (lq *LockQueue[T]) Size() int {
	g := NewGuard(&lq.mu)
	defer g.Done()
	return len(lq.queue)
}

// ---------------- LockPriorityQueue ----------------

type HasID interface {
	GetID() int
}

type pqItem[T HasID] struct {
	value *T
}

type priorityQueue[T HasID] []*T

func (pq priorityQueue[T]) Len() int { return len(pq) }
func (pq priorityQueue[T]) Less(i, j int) bool {
	return (*pq[i]).GetID() < (*pq[j]).GetID()
}
func (pq priorityQueue[T]) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }

func (pq *priorityQueue[T]) Push(x any) {
	item := x.(*T)
	*pq = append(*pq, item)
}

func (pq *priorityQueue[T]) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[:n-1]
	return item
}

type LockPriorityQueue[T HasID] struct {
	mu    SpinLock
	queue priorityQueue[T]
}

func (lpq *LockPriorityQueue[T]) Pop() *T {
	g := NewGuard(&lpq.mu)
	defer g.Done()

	if len(lpq.queue) == 0 {
		return nil
	}
	item := heap.Pop(&lpq.queue).(*T)
	return item
}

func (lpq *LockPriorityQueue[T]) Push(tx *T) {
	g := NewGuard(&lpq.mu)
	defer g.Done()
	heap.Push(&lpq.queue, tx)
}

func (lpq *LockPriorityQueue[T]) Size() int {
	g := NewGuard(&lpq.mu)
	defer g.Done()
	return lpq.queue.Len()
}

// ---------------- Table ----------------

type Hasher[K comparable] interface {
	Hash(K) uint64
}

type Table[K comparable, V any] struct {
	numPartitions int
	locks         []SpinLock
	partitions    []map[K]V
	hasher        Hasher[K]
}

func NewTable[K comparable, V any](num int, hasher Hasher[K]) *Table[K, V] {
	locks := make([]SpinLock, num)
	parts := make([]map[K]V, num)
	for i := range parts {
		parts[i] = make(map[K]V)
	}
	return &Table[K, V]{
		numPartitions: num,
		locks:         locks,
		partitions:    parts,
		hasher:        hasher,
	}
}

func (t *Table[K, V]) Get(k K, vmap func(V)) {
	partitionID := int(t.hasher.Hash(k)) % t.numPartitions
	log.Printf("Get key: %v at partition %d", k, partitionID)

	g := NewGuard(&t.locks[partitionID])
	defer g.Done()

	partition := t.partitions[partitionID]
	if v, ok := partition[k]; ok {
		vmap(v)
	} else {
		log.Printf("key not found")
	}
}

func (t *Table[K, V]) Put(k K, vmap func(*V)) {
	partitionID := int(t.hasher.Hash(k)) % t.numPartitions
	log.Printf("Put key: %v at partition %d", k, partitionID)

	g := NewGuard(&t.locks[partitionID])
	defer g.Done()

	partition := t.partitions[partitionID]
	v, exists := partition[k]
	if !exists {
		var zero V
		partition[k] = zero
		v = zero
	}
	vmap(&v)
	partition[k] = v
}
