package serial

import (
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"fmt"
	"time"

	"github.com/holiman/uint256"
)

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) float64 {
	fmt.Println("=== Run Serial ===")

	start2 := time.Now()
	for _, txs := range blockTxs {
		levmCopy := levm.Copy()
		for _, tx := range txs {
			_, err := levmCopy.CallContract(*tx.From(), *tx.To(), tx.Data(), new(uint256.Int).SetUint64(0))
			tools.PanicError("Serial Tx Execute ", err)
		}
	}
	end2 := time.Since(start2)
	txNumber := janusConfig.TxNum
	tps := float64(txNumber) / end2.Seconds()
	fmt.Println("Serial TPS:", tps)
	fmt.Printf("Serial Execution Time:     %-22v \n", end2)
	return tps
}
