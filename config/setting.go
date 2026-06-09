package config

import (
	"os"
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

	AllThreadNum   = 8
	AllBlocksTxSum = 4000
	BlockSize      = 2000
	Skew           = 1.01

	WaterMarkAlpha = 3.5 // 水位线参数 α
	WaterMarkBeta  = 5.5 // 水位线参数 β
)

// 交易生成相关配置
const (
	AddressNumberRate    = 4 // 总共生成多少个地址, 按单个区块交易数量的（比例）来生成
	CompetingTxCountRate = 0.5
	IoTxCountRate        = 0.5
)

// 斐波那契计算相关配置
const (
	FibonacciN                  = 10
	FibonacciM                  = 500
	RecursiveCalculateFibonacci = false // 是否使用递归计算斐波那契
)

var (
	// MonitorBasePath 是实验监控和 TPS 汇总文件的根目录。
	// 启动时自动识别当前用户目录，并在该目录下使用 cpu_disk_monitor，避免不同机器硬编码 /home/bcds 或 /root。
	MonitorBasePath string
)

const (
	BlockSum     = 200   // 执行多少个区块
	ChanLen      = 20000 // 每个区块有多少笔交易
	TxSum        = 20000 // 每个区块有多少笔交易
	OpenReadFile = true
)

const (
	LongTx                 TransactionType = 1
	ShortTx                TransactionType = 2
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

var (
	InstructionTimingFilePath string
	SmallbankDatabasePath     string
)

func init() {
	// 在 init 中计算项目根目录
	_, filename, _, _ := runtime.Caller(0)
	ProjectRoot := filepath.Dir(filepath.Dir(filename))
	InstructionTimingFilePath = filepath.Join(ProjectRoot, "config", "instruction_timings.json")
	SmallbankDatabasePath = filepath.Join(ProjectRoot, "data", "smallbank_database")
	homeDir, err := os.UserHomeDir()
	if err != nil || homeDir == "" {
		homeDir = ProjectRoot
	}
	MonitorBasePath = filepath.Join(homeDir, "cpu_disk_monitor")
	_ = os.MkdirAll(MonitorBasePath, 0755)
}
