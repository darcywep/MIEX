package tools

var (
	CatStorageState         = false
	JournalNonce            = false
	LoadTxCost              = false
	TxCost          float64 = 0.0
)

func InitTxCost() {
	LoadTxCost = true
	TxCost = 0.0
}
