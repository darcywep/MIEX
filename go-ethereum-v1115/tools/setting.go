package tools

var (
	AllThreadNum = 8
	TxNum        = 4000
	Skew         = 1.01
)

// 交易生成相关配置
const (
	BlockSize = 2000

	AddressNumberRate    = 2 // 总共生成多少个地址, 按单个区块交易数量的（比例）来生成
	CompetingTxCountRate = 0.5
	IoTxCountRate        = 0.5
)

// 斐波那契计算相关配置
const (
	FibonacciN                  = 10
	RecursiveCalculateFibonacci = true // 是否使用递归计算斐波那契
)

const (
	MonitorBasePath = "/root/cpu_disk_monitor/"
)

type TransactionType uint64

const (
	ComputeTx TransactionType = 1
	IOTx      TransactionType = 2
)
