package config

import "github.com/syndtr/goleveldb/leveldb/opt"

var Options = &opt.Options{
	//BlockCacheCapacity: 0, // 禁用 block cache
	//WriteBuffer:        0, // 禁用写缓冲
	//Strict:             opt.DefaultStrict,
}

var (
	IoThreadNum        int = 4
	ComputingThreadNum int = 4
)

const (
	AddressNumber            = 1000 // 总共生成多少个地址, 这用于添加到交易中
	CompetingTxCountForBlock = 1000
	IoTxCountForBlock        = 0
	Skew                     = 1.01
)

const (
	AllThreadNum = 8
	BlockSum     = 200   // 执行多少个区块
	ChanLen      = 20000 // 每个区块有多少笔交易
	TxSum        = 20000 // 每个区块有多少笔交易
	OpenReadFile = false
)

const (
	ComputeTx              TransactionType = 1
	IOTx                   TransactionType = 2
	TotalKeys                              = 25 * 10000 * 10000 // LevelDB 总数据量
	JanusDBPath                            = "/root/alldb/JanusDB"
	Key2addrDBPath                         = "/root/alldb/key2addrDB"
	MonitorFilenameSep                     = "./cpu_disk_monitor/cpu_disk_Sep"
	MonitorFilenameHybrid                  = "./cpu_disk_monitor/cpu_disk_Hybrid"
	MonitorFilenameCompute                 = "./cpu_disk_monitor/cpu_disk_Compute"
	MonitorFilenameIO                      = "./cpu_disk_monitor/cpu_disk_IO"
	MonitorFilenamePeep                    = "./cpu_disk_monitor/cpu_disk_Peep"
)

const (
	FilePath   = "./file"
	FibonacciN = 350
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
