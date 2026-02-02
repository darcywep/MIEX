package config

import (
	"path/filepath"
	"runtime"

	"github.com/syndtr/goleveldb/leveldb/opt"
)

var Options = &opt.Options{
	//BlockCacheCapacity: 0, // 禁用 block cache
	//WriteBuffer:        0, // 禁用写缓冲
	//Strict:             opt.DefaultStrict,
}

var (
	IoThreadNum        int = 4
	ComputingThreadNum int = 4

	AllThreadNum = 8
	TxNum        = 4000
	Skew         = 1.01
)

// 交易生成相关配置
const (
	BlockSize = 2000

	AddressNumberRate    = 4 // 总共生成多少个地址, 按单个区块交易数量的（比例）来生成
	CompetingTxCountRate = 0.5
	IoTxCountRate        = 0.5

	WaterMarkAlpha = 1.5 // 水位线参数 α
	WaterMarkBeta  = 3.5 // 水位线参数 β
)

// 斐波那契计算相关配置
const (
	FibonacciN                  = 10
	FibonacciM                  = 500
	RecursiveCalculateFibonacci = false // 是否使用递归计算斐波那契
)

const (
	MonitorBasePath = "/root/cpu_disk_monitor/"
)

const (
	BlockSum     = 200   // 执行多少个区块
	ChanLen      = 20000 // 每个区块有多少笔交易
	TxSum        = 20000 // 每个区块有多少笔交易
	OpenReadFile = true
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
	FilePath = "./file"

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

var InstructionTimingFilePath string

func init() {
	// 在 init 中计算项目根目录
	_, filename, _, _ := runtime.Caller(0)
	ProjectRoot := filepath.Dir(filepath.Dir(filename))
	InstructionTimingFilePath = filepath.Join(ProjectRoot, "config", "instruction_timings.json")
}
