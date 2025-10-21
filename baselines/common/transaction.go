package common

import (
	"fmt"
)

// SetStorage 定义设置存储的处理函数类型
type SetStorage func(writeSet map[string]bool, value string)

// GetStorage 定义获取存储的处理函数类型
type GetStorage func(readSet map[string]bool)

// Transaction 事务结构体
type Transaction struct {
	tx         *HyperVertex
	setHandler SetStorage
	getHandler GetStorage
}

// NewTransaction 创建新事务
func NewTransaction(tx *HyperVertex) *Transaction {
	return &Transaction{
		tx: tx,
	}
}

// InstallSetStorageHandler 安装设置存储的处理函数
func (t *Transaction) InstallSetStorageHandler(handler SetStorage) {
	t.setHandler = handler
}

// InstallGetStorageHandler 安装获取存储的处理函数
func (t *Transaction) InstallGetStorageHandler(handler GetStorage) {
	t.getHandler = handler
}

// Execute 执行事务
func (t *Transaction) Execute() {
	fmt.Printf("Execute transaction: %p txid: %d\n", t.tx, t.tx.HyperId)

	if t.getHandler != nil {
		t.getHandler(t.tx.RootVertex.AllReadSet)
	}

	if t.setHandler != nil {
		t.setHandler(t.tx.RootVertex.AllWriteSet, "value")
	}

	// 执行事务成本计算
	Exec(t.tx.RootVertex.Cost)
}

// CountOverheads 计算开销
func (t *Transaction) CountOverheads() int {
	return t.tx.RootVertex.Cost
}

// GetTx 获取事务关联的HyperVertex
func (t *Transaction) GetTx() *HyperVertex {
	return t.tx
}
