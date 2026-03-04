package common

import (
	"janus-geth-1165/ethereum/core/types"
)

type BasicTransaction struct {
	Txid   uint32
	Vertex *TransactionVertex
	EthTx  *types.Transaction
}

type TransactionVertex struct {
	ReadKeys  map[string]string
	WriteKeys map[string]string
	Children  map[*TransactionVertex]bool
}

func NewBasicTransaction(txid uint32, vertex *TransactionVertex, ethTx *types.Transaction) *BasicTransaction {
	return &BasicTransaction{Txid: txid, Vertex: vertex, EthTx: ethTx}
}
