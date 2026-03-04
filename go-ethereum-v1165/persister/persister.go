package persister

import (
	"Janus/config"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Persister struct {
	DB *leveldb.DB
}

// NewPersister 打开 LevelDB，缓存为 0
func NewPersister(path string) (*Persister, error) {
	//_ = os.MkdirAll(path, 0755)
	db, err := leveldb.OpenFile(path, config.Options)
	if err != nil {
		return nil, err
	}

	return &Persister{DB: db}, nil
}

// Put 写入单条数据
func (p *Persister) Put(key, value []byte) error {
	if p.DB == nil {
		return errors.New("db is nil")
	}
	return p.DB.Put(key, value, &opt.WriteOptions{Sync: true})
}

// BatchPut 批量写入（保证原子性）
func (p *Persister) BatchPut(kvs map[string][]byte) error {
	if p.DB == nil {
		return errors.New("db is nil")
	}
	batch := new(leveldb.Batch)
	for k, v := range kvs {
		batch.Put([]byte(k), v)
	}
	return p.DB.Write(batch, &opt.WriteOptions{Sync: true})
}

// Get 读取单条数据
func (p *Persister) Get(key []byte) ([]byte, error) {
	if p.DB == nil {
		return nil, errors.New("db is nil")
	}
	val, err := p.DB.Get(key, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, nil // key 不存在返回 nil
	}
	return val, err
}

// Has 判断 key 是否存在
func (p *Persister) Has(key []byte) (bool, error) {
	if p.DB == nil {
		return false, errors.New("db is nil")
	}
	return p.DB.Has(key, nil)
}

// Delete 删除单条数据
func (p *Persister) Delete(key []byte) error {
	if p.DB == nil {
		return errors.New("db is nil")
	}
	return p.DB.Delete(key, &opt.WriteOptions{Sync: true})
}

// Iterate 遍历一个范围内的 key
func (p *Persister) Iterate(start, limit []byte, fn func(k, v []byte)) error {
	if p.DB == nil {
		return errors.New("db is nil")
	}
	iter := p.DB.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	defer iter.Release()

	for iter.Next() {
		fn(iter.Key(), iter.Value())
	}
	return iter.Error()
}

// GetSnapshot 返回一个快照，用于并发查询
func (p *Persister) GetSnapshot() (*leveldb.Snapshot, error) {
	if p.DB == nil {
		return nil, errors.New("db is nil")
	}
	return p.DB.GetSnapshot()
}

func (p *Persister) ReleaseSnapshot(snapshot *leveldb.Snapshot) {
	snapshot.Release()
}

// Close 关闭数据库
func (p *Persister) Close() error {
	if p.DB == nil {
		return nil
	}
	return p.DB.Close()
}
