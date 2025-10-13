package config

// TransactionType 定义交易类型：0=计算型，1=I/O型
type TransactionType int

const (
	ComputeTx TransactionType = 1
	IOTx      TransactionType = 2
	TotalKeys                 = 25 * 10000 * 10000 // LevelDB 总数据量
)

// KV 表示要更新的键值对
type KV struct {
	Key   string
	Value []byte
}

// Transaction 表示一笔交易
type Transaction struct {
	ID       string
	Type     TransactionType
	Updates  []KV  // 需要更新的多个 key
	Executed bool  // 是否已执行
	Success  bool  // 执行是否成功
	Error    error // 执行时的错误（可选）
	ReadKey  []string
}
