package common

import "fmt"

type JanusTransaction struct {
	Txid   uint32
	Vertex *TransactionVertex
}

type TransactionVertex struct {
	ReadKeys  map[string]string
	WriteKeys map[string]string
	Children  map[*TransactionVertex]bool
}

func NewJanusTransaction(txid uint32, vertex *TransactionVertex) *JanusTransaction {
	return &JanusTransaction{txid, vertex}
}

func NewTransactionVertex(readKeys map[string]string, writeKeys map[string]string, children map[*TransactionVertex]bool) *TransactionVertex {
	return &TransactionVertex{readKeys, writeKeys, children}
}

func (t *JanusTransaction) Execute() {
	fmt.Printf("模拟执行交易 %d", t.Txid)
}
