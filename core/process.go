package core

import (
	"errors"
	"log"
	"math/rand"
	"time"

	"Janus/config"
	"Janus/persister"

	"github.com/syndtr/goleveldb/leveldb"
)

// Fibonacci: 模拟计算密集任务
func fibonacci(n int) int {
	if n <= 1 {
		return n
	}
	return fibonacci(n-1) + fibonacci(n-2)
}

func executeCompetingTransaction(cache *persister.StateCache, tx *config.Transaction) {
	start := time.Now()

	n := rand.Intn(30) + 10 // 随机计算 Fibonacci(10~40)
	_ = fibonacci(n)

	// 模拟写入执行结果
	for _, key := range tx.Updates {
		cache.Put(tx.ID, []byte(key.Key), key.Value)
	}
	tx.Executed = true
	tx.Success = true
	tx.Error = nil

	log.Printf("✅ 交易 %s 执行完成 (type=%d)，耗时=%s", tx.ID, tx.Type, time.Since(start))
}

func executeIOTransaction(cache *persister.StateCache, tx *config.Transaction) {
	start := time.Now()
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

	log.Printf("✅ 交易 %s 执行完成 (type=%d)，耗时=%s", tx.ID, tx.Type, time.Since(start))
}

// ExecuteTransaction 执行一笔交易
func ExecuteTransaction(cache *persister.StateCache, tx *config.Transaction) {
	if config.ComputeTx == tx.Type {
		executeCompetingTransaction(cache, tx)
	} else {
		executeIOTransaction(cache, tx)
	}
}
