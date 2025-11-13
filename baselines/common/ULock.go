package common

import (
	"hash/fnv"
	"log"
	"runtime"
	"sync/atomic"
)

// SpinLock 自旋锁
type SpinLock struct {
	flag atomic.Bool
}

// Lock 加锁
func (s *SpinLock) Lock() {
	for !s.flag.CompareAndSwap(false, true) {
		runtime.Gosched() // 让出CPU时间片
	}
}

// Unlock 解锁
func (s *SpinLock) Unlock() {
	s.flag.Store(false)
}

// Guard 守卫，用于自动加锁解锁
type Guard struct {
	lock *SpinLock
}

// NewGuard 创建守卫
func NewGuard(lock *SpinLock) *Guard {
	lock.Lock()
	return &Guard{lock: lock}
}

// Release 释放锁
func (g *Guard) Release() {
	if g.lock != nil {
		g.lock.Unlock()
		g.lock = nil
	}
}

// LockQueue 并发队列
type LockQueue[T any] struct {
	mu    SpinLock
	queue []T
}

// NewLockQueue 创建新的并发队列
func NewLockQueue[T any]() *LockQueue[T] {
	return &LockQueue[T]{
		queue: make([]T, 0),
	}
}

// Pop 弹出元素
func (q *LockQueue[T]) Pop() (T, bool) {
	guard := NewGuard(&q.mu)
	defer guard.Release()

	if len(q.queue) == 0 {
		var zero T
		return zero, false
	}

	item := q.queue[0]
	q.queue = q.queue[1:]
	return item, true
}

// Push 推入元素
func (q *LockQueue[T]) Push(item T) {
	guard := NewGuard(&q.mu)
	defer guard.Release()

	q.queue = append(q.queue, item)
}

// Size 获取队列大小
func (q *LockQueue[T]) Size() int {
	guard := NewGuard(&q.mu)
	defer guard.Release()

	return len(q.queue)
}

// LockPriorityQueue 并发优先队列
type LockPriorityQueue[T any] struct {
	mu    SpinLock
	queue []T
	less  func(a, b T) bool
}

// NewLockPriorityQueue 创建新的并发优先队列
func NewLockPriorityQueue[T any](less func(a, b T) bool) *LockPriorityQueue[T] {
	return &LockPriorityQueue[T]{
		queue: make([]T, 0),
		less:  less,
	}
}

// Pop 弹出最小元素
func (q *LockPriorityQueue[T]) Pop() (T, bool) {
	guard := NewGuard(&q.mu)
	defer guard.Release()

	if len(q.queue) == 0 {
		var zero T
		return zero, false
	}

	// 找到最小元素
	minIndex := 0
	for i := 1; i < len(q.queue); i++ {
		if q.less(q.queue[i], q.queue[minIndex]) {
			minIndex = i
		}
	}

	item := q.queue[minIndex]
	// 移除最小元素
	q.queue = append(q.queue[:minIndex], q.queue[minIndex+1:]...)
	return item, true
}

// Push 推入元素
func (q *LockPriorityQueue[T]) Push(item T) {
	guard := NewGuard(&q.mu)
	defer guard.Release()

	q.queue = append(q.queue, item)
}

// Size 获取队列大小
func (q *LockPriorityQueue[T]) Size() int {
	guard := NewGuard(&q.mu)
	defer guard.Release()

	return len(q.queue)
}

// KeyHasher 键哈希函数
type KeyHasher struct{}

// Hash 计算字符串的哈希值
func (h KeyHasher) Hash(key string) uint32 {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	return hasher.Sum32()
}

// Table 并发哈希表
type Table[V any] struct {
	numPartitions int
	locks         []*SpinLock
	partitions    []map[string]V
	hasher        KeyHasher
}

// NewTable 创建新的并发哈希表
func NewTable[V any](partitions int) *Table[V] {
	locks := make([]*SpinLock, partitions)
	partitionMaps := make([]map[string]V, partitions)

	for i := 0; i < partitions; i++ {
		locks[i] = &SpinLock{}
		partitionMaps[i] = make(map[string]V)
	}

	return &Table[V]{
		numPartitions: partitions,
		locks:         locks,
		partitions:    partitionMaps,
		hasher:        KeyHasher{},
	}
}

// Get 获取值
func (t *Table[V]) Get(key string, vmap func(value V)) {
	partitionID := t.hasher.Hash(key) % uint32(t.numPartitions)

	guard := NewGuard(t.locks[partitionID])
	defer guard.Release()

	partition := t.partitions[partitionID]
	if value, exists := partition[key]; exists {
		vmap(value)
	} else {
		log.Printf("key not found")
	}
}

// Put 设置值
func (t *Table[V]) Put(key string, vmap func(value *V)) {
	partitionID := t.hasher.Hash(key) % uint32(t.numPartitions)
	guard := NewGuard(t.locks[partitionID])
	defer guard.Release()

	//fmt.Printf("t.partitions = %v\n", t.partitions)

	partition := t.partitions[partitionID]
	value := partition[key]

	//fmt.Printf("11111111 \n")
	//fmt.Printf("partitionID = %v\n", partitionID)
	//fmt.Printf("value: %v\n", value)

	// 使用临时变量，可以取地址
	vmap(&value)

	// 更新回map
	partition[key] = value
}

// PutWithDefault 设置值（带默认值）
func (t *Table[V]) PutWithDefault(key string, defaultValue V, vmap func(value *V)) {
	partitionID := t.hasher.Hash(key) % uint32(t.numPartitions)

	guard := NewGuard(t.locks[partitionID])
	defer guard.Release()

	partition := t.partitions[partitionID]

	if _, exists := partition[key]; !exists {
		partition[key] = defaultValue
	}

	value := partition[key]

	// 使用临时变量，可以取地址
	vmap(&value)

	// 更新回map
	partition[key] = value
}
