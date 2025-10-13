package persister

import (
	"Janus/config"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type WriteEntry struct {
	TxID  string // 哪个交易写的
	Value []byte
}

type StateCache struct {
	key2AddrDB *leveldb.DB
	data       map[string]WriteEntry
	dataMtx    sync.RWMutex

	key2addr    map[string]string
	key2addrMtx sync.RWMutex

	persister *Persister
	snapshot  *leveldb.Snapshot
}

func NewStateCache(JanusDBPath, key2addrDBPath string) *StateCache {
	persister, err := NewPersister(JanusDBPath)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	snapshot, err := persister.GetSnapshot()
	if err != nil {
		fmt.Println(err)
	}
	key2AddrDB, err := leveldb.OpenFile(key2addrDBPath, config.Options)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	return &StateCache{
		data:       make(map[string]WriteEntry),
		persister:  persister,
		snapshot:   snapshot,
		key2AddrDB: key2AddrDB,
		key2addr:   make(map[string]string),
	}
}

// Get 查询：优先查缓存
func (c *StateCache) Get(key string) ([]byte, error) {
	c.dataMtx.RLock()
	val, ok := c.data[key]
	c.dataMtx.RUnlock()
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
	nKey, err := c.key2AddrDB.Get([]byte(key), nil)
	if nKey == nil {
		return nil, err
	}
	address := common.BytesToAddress(nKey).Hex()
	nVal, err := c.persister.Get(nKey)
	if err != nil {
		return nil, err
	}

	c.dataMtx.Lock()
	c.data[key] = WriteEntry{"", nVal}
	c.key2addr[key] = address
	c.dataMtx.Unlock()

	return nVal, nil
}

// Put 写入：只写内存缓存
func (c *StateCache) Put(txID string, key, value []byte) {
	c.dataMtx.Lock()
	c.data[string(key)] = WriteEntry{TxID: txID, Value: value}
	c.dataMtx.Unlock()
}

// BatchPut 写入：只写内存缓存
func (c *StateCache) BatchPut(entries *[]WriteEntry) {
	c.dataMtx.Lock()
	for _, entry := range *entries {
		c.data[string(entry.TxID)] = WriteEntry{TxID: entry.TxID, Value: entry.Value}
	}
	c.dataMtx.Unlock()
}

// Commit 批量提交：把缓存写入 DB
func (c *StateCache) Commit() {
	c.dataMtx.Lock()
	defer c.dataMtx.Unlock()

	batch := new(leveldb.Batch)
	for k, v := range c.data {
		address := c.key2addr[k]
		batch.Put([]byte(address), v.Value)
	}
	fmt.Println(len(c.data))
	c.data = make(map[string]WriteEntry)
	c.key2addr = make(map[string]string)
	_ = c.persister.DB.Write(batch, &opt.WriteOptions{Sync: true})
}

func (c *StateCache) Close() {
	c.persister.ReleaseSnapshot(c.snapshot)
	_ = c.persister.Close()
	_ = c.key2AddrDB.Close()
}
