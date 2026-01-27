package tools

var (
	CatStorageState        = false
	JournalNonce           = false
	Uint64          uint64 = 0xFFFFFFFFFFFFFFFF

	LoadTxCost []bool
	TxCost     []float64
)

func init() {
	LoadTxCost = make([]bool, 1)
	TxCost = make([]float64, 1)
}

func InitTxCost(threads int) {
	LoadTxCost = make([]bool, threads)
	TxCost = make([]float64, threads)
}

func OpenTxCost(workerID int) {
	LoadTxCost[workerID] = true
	TxCost[workerID] = 0.0
}

func CloseTxCost(workerID int) {
	LoadTxCost[workerID] = false
}

func AddTxCost(workerID int, cost float64) {
	TxCost[workerID] += cost
}
