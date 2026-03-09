package core

import (
	"errors"
	"fmt"
	"Janus/config"
	"Janus/file"
	"Janus/persister"
	"log"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
)

// Fibonacci: 模拟计算密集任务
func fibonacci(n int) int {
	//time.Sleep(1 * time.Microsecond)
	if n <= 1 {
		return n
	}
	return fibonacci(n-1) + fibonacci(n-2)
}

func ExecuteCompetingTransaction(cache *persister.StateCache, tx *config.Transaction) {
	//start := time.Now()
	_ = fibonacci(config.FibonacciN)

	// 模拟写入执行结果
	for _, key := range tx.Updates {
		cache.Put(tx.ID, []byte(key.Key), key.Value)
	}
	tx.Executed = true
	tx.Success = true
	tx.Error = nil

	//log.Printf("✅ 交易 %s 执行完成 (type=%d)，耗时=%s", tx.ID, tx.Type, time.Since(start))
}

func ExecuteIOTransaction(cache *persister.StateCache, tx *config.Transaction, i int) {
	//start := time.Now()
	for _, key := range tx.ReadKey {
		_, err := cache.Get(key)
		if err != nil && !errors.Is(err, leveldb.ErrNotFound) {
			log.Printf("⚠️ I/O 交易 %s 读取失败: %v", tx.ID, err)
		}
	}

	// 模拟写入执行结果
	for _, key := range tx.Updates {
		cache.Put(tx.ID, []byte(key.Key), key.Value)
	}

	tx.Executed = true
	tx.Success = true
	tx.Error = nil

	// 读一次触发IO中断
	if config.OpenReadFile {
		name := filepath.Join(config.FilePath, fmt.Sprintf("file_%03d.bin", i))
		if err := file.ReadOnce(name); err != nil {
			fmt.Printf("读取 %s 失败: %v\n", name, err)
		}
	}
	//log.Printf("✅ 交易 %s 执行完成 (type=%d)，耗时=%s", tx.ID, tx.Type, time.Since(start))
}

// ExecuteTransaction 执行一笔交易
func ExecuteTransaction(cache *persister.StateCache, tx *config.Transaction, i int) {
	if config.LongTx == tx.Type {
		//time.Sleep(1 * time.Microsecond)
		ExecuteCompetingTransaction(cache, tx)
	} else {
		//time.Sleep(1 * time.Microsecond)
		ExecuteIOTransaction(cache, tx, i)
	}
}
