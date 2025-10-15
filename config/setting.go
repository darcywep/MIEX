package config

import "github.com/syndtr/goleveldb/leveldb/opt"

var Options = &opt.Options{
	//BlockCacheCapacity: 0, // 禁用 block cache
	//WriteBuffer:        0, // 禁用写缓冲
	//Strict:             opt.DefaultStrict,
}

const (
	AllThreadNum       = 8
	IoThreadNum        = 1
	ComputingThreadNum = 7
	BlockSum           = 100   // 执行多少个区块
	ChanLen            = 20000 // 每个区块有多少笔交易
	TxSum              = 20000 // 每个区块有多少笔交易
)

const (
	ComputeTx              TransactionType = 1
	IOTx                   TransactionType = 2
	TotalKeys                              = 25 * 10000 * 10000 // LevelDB 总数据量
	JanusDBPath                            = "./JanusDB"
	Key2addrDBPath                         = "./key2addrDB"
	MonitorFilenameSep                     = "./cpu_disk_monitor/cpu_disk_Sep.xlsx"
	MonitorFilenameHybrid                  = "./cpu_disk_monitor/cpu_disk_Hybrid.xlsx"
	MonitorFilenameCompute                 = "./cpu_disk_monitor/cpu_disk_Compute.xlsx"
	MonitorFilenameIO                      = "./cpu_disk_monitor/cpu_disk_IO.xlsx"
)

const (
	FilePath   = "./file"
	FibonacciN = 15
	//n := rand.Intn(30) + 10 // 随机计算 Fibonacci(10~40)
	//n := rand.Intn(10) // 随机计算 Fibonacci(10~40)
)

const (
	CompetingTxCount = 2000 * 10000 // 计算型交易数
	IoTxCount        = 2000 * 10000 // IO 型交易数
	ComputingWriteN  = 1
	ComputingReadN   = 0
	IoWriteN         = 2
	IoReadN          = 0
	CalcKeysPerTx    = ComputingWriteN + ComputingReadN // 每个计算交易读的 key 数
	IoKeysPerTx      = IoWriteN + IoReadN               // 每个 IO 交易读的 key 数)
)
