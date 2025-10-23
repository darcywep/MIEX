package Common

import "fmt"

type JanusTransaction struct {
	Txid   string
	Cost   int
	Vertex *TransactionVertex
}

type TransactionVertex struct {
	ReadKeys  map[string]bool
	WriteKeys map[string]bool
	Children  map[*TransactionVertex]bool
}

func NewJanusTransaction(txid string, vertex *TransactionVertex, cost int) *JanusTransaction {
	return &JanusTransaction{txid, cost, vertex}
}

func NewTransactionVertex(readKeys map[string]bool, writeKeys map[string]bool, children map[*TransactionVertex]bool) *TransactionVertex {
	return &TransactionVertex{readKeys, writeKeys, children}
}

func (t *JanusTransaction) Execute() {
	fmt.Printf("模拟执行交易 %d", t.Txid)
}
