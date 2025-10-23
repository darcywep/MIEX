package Optme

import (
	"Janus/plugin/Common"
	"strconv"
)

type TxGenerator struct {
	txNum     int
	blockSize int
	blocks    []*Common.Block
}

func NewTxGenerator(txNum int, blockSize int) *TxGenerator {
	return &TxGenerator{txNum, blockSize, nil}
}

func (g *TxGenerator) GenerateTransaction(txid string, cost int, readKeys map[string]bool, writekeys map[string]bool) *Common.JanusTransaction {
	vertex := Common.TransactionVertex{readKeys, writekeys, nil}
	return Common.NewJanusTransaction(txid, &vertex, cost)
}

func (tg *TxGenerator) GenerateBlock(blockID int) *Common.Block {
	txs := make([]*Common.JanusTransaction, 0)

	for i := 0; i < tg.blockSize; i++ {
		txid := blockID*tg.blockSize + i + 1
		readKeys := make(map[string]bool)
		writekeys := make(map[string]bool)

		tx := tg.GenerateTransaction(strconv.Itoa(txid), 0, readKeys, writekeys)
		txs = append(txs, tx)
	}
	return Common.NewBlock(blockID, txs)
}

func (tg *TxGenerator) GenerateWorkload() []*Common.Block {
	blocks := make([]*Common.Block, 0)
	blockNum := tg.txNum / tg.blockSize

	for i := 0; i < blockNum; i++ {
		block := tg.GenerateBlock(i)
		blocks = append(blocks, block)
	}
	return blocks
}
