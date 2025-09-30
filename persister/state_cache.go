package persister

import (
	"fmt"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type WriteEntry struct {
	TxID  string // 哪个交易写的
	Value []byte
}

type StateCache struct {
	data      map[string]WriteEntry
	persister *Persister
	snapshot  *leveldb.Snapshot
	mu        sync.RWMutex
}

func NewStateCache(leveldbPath string) *StateCache {
	persister, err := NewPersister(leveldbPath)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	snapshot, err := persister.GetSnapshot()
	if err != nil {
		fmt.Println(err)
	}
	return &StateCache{
		data:      make(map[string]WriteEntry),
		persister: persister,
		snapshot:  snapshot,
	}
}

// Get 查询：优先查缓存
func (c *StateCache) Get(key string) ([]byte, error) {
	c.mu.RLock()
	val, ok := c.data[key]
	c.mu.RUnlock()
	if ok {
		return val.Value, nil
	}
	//value, err := c.snapshot.Get([]byte(key), nil)
	//if err != nil {
	//	return nil, err
	//}
	//if value != nil {
	//	return value, nil
	//}
	return c.persister.Get([]byte(key))
}

// Put 写入：只写内存缓存
func (c *StateCache) Put(txID string, key, value []byte) {
	c.mu.Lock()
	c.data[string(key)] = WriteEntry{TxID: txID, Value: value}
	c.mu.Unlock()
}

// BatchPut 写入：只写内存缓存
func (c *StateCache) BatchPut(entries *[]WriteEntry) {
	c.mu.Lock()
	for _, entry := range *entries {
		c.data[string(entry.TxID)] = WriteEntry{TxID: entry.TxID, Value: entry.Value}
	}
	c.mu.Unlock()
}

// Commit 批量提交：把缓存写入 DB
func (c *StateCache) Commit() {
	c.mu.Lock()
	defer c.mu.Unlock()

	batch := new(leveldb.Batch)
	for k, v := range c.data {
		batch.Put([]byte(k), v.Value)
	}
	fmt.Println(len(c.data))
	c.data = make(map[string]WriteEntry)
	_ = c.persister.DB.Write(batch, &opt.WriteOptions{Sync: true})
}

func (c *StateCache) Close() {
	c.persister.ReleaseSnapshot(c.snapshot)
	_ = c.persister.Close()

}
