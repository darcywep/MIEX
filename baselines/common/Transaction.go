package common

import (
	"Janus/ethereum/core/types"
)

type BasicTransaction struct {
	Txid         uint32
	OriginalTxID int
	Vertex       *TransactionVertex
	EthTx        *types.Transaction
}

type TransactionVertex struct {
	ReadKeys  map[string]string
	WriteKeys map[string]string
	Children  map[*TransactionVertex]bool
}

func NewBasicTransaction(txid uint32, originalTxID int, vertex *TransactionVertex, ethTx *types.Transaction) *BasicTransaction {
	return &BasicTransaction{Txid: txid, OriginalTxID: originalTxID, Vertex: vertex, EthTx: ethTx}
}
