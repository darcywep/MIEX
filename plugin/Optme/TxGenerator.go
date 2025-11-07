package Optme

import (
	"Janus/plugin/Common"
)

type TxGenerator struct {
	txNum     int
	blockSize int
	blocks    []*Common.Block
}

func NewTxGenerator(txNum int, blockSize int) *TxGenerator {
	return &TxGenerator{txNum, blockSize, nil}
}

func (g *TxGenerator) GenerateTransaction(txid uint32, cost uint32, readKeys map[string]string, writeKeys map[string]string) *Common.JanusTransaction {
	vertex := Common.TransactionVertex{readKeys, writeKeys, nil}
	return Common.NewJanusTransaction(txid, &vertex, cost)
}

func (tg *TxGenerator) GenerateBlock(blockID int) *Common.Block {

	txs := make([]*Common.JanusTransaction, 0)

	for i := 0; i < tg.blockSize; i++ {

		txid := blockID*tg.blockSize + i + 1
		readKeys := make(map[string]string)
		writeKeys := make(map[string]string)

		tx := tg.GenerateTransaction(uint32(txid), uint32(Common.TX_COST), readKeys, writeKeys)
		txs = append(txs, tx)
	}
	return Common.NewBlock(blockID, txs)
}

func (tg *TxGenerator) GenerateWorkload() []*Common.Block {
	blocks := make([]*Common.Block, 0)
	blockNum := tg.txNum / tg.blockSize // 区块数目

	for i := 0; i < blockNum; i++ {
		block := tg.GenerateBlock(i)   // 生成区块，i为区块号
		blocks = append(blocks, block) // 添加区块至 blocks
	}
	return blocks
}
