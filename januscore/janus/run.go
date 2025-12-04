package janus

import (
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"fmt"
	"time"
)

func Run(blockTxs []types.Transactions, levm *lvm.LEVM) float64 {
	fmt.Println("=== Run SChain ===")

	blockNum := janusConfig.TxNum / janusConfig.BlockSize

	start2 := time.Now()
	for i := 0; i < blockNum; i++ {
		txs := blockTxs[i]
		// Todo: implement Janus run logic here

	}
	end2 := time.Since(start2)
	txNumber := janusConfig.TxNum
	tps := float64(txNumber) / end2.Seconds()
	fmt.Println("SChian TPS:", tps)
	return tps
}
