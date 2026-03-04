package common

import (
	"janus-geth-1165/ethereum/core/types"
)

type TxGenerator struct {
	txNum     int
	blockSize int
	blocks    []*Block
}

func NewTxGenerator(txNum int, blockSize int) *TxGenerator {
	return &TxGenerator{txNum, blockSize, nil}
}

func (g *TxGenerator) GenerateTransaction(txid uint32, readKeys map[string]string, writeKeys map[string]string, ethTx *types.Transaction) *BasicTransaction {
	vertex := TransactionVertex{readKeys, writeKeys, nil}
	return NewBasicTransaction(txid, &vertex, ethTx)
}

func (tg *TxGenerator) GenerateBlock(blockID int, ethTxs types.Transactions) *Block { // 生成的区块中包含EVM可执行交易
	txs := make([]*BasicTransaction, 0)

	for i := 0; i < tg.blockSize; i++ {
		txid := blockID*tg.blockSize + i + 1
		readKeys := make(map[string]string)
		writeKeys := make(map[string]string)

		tx := tg.GenerateTransaction(uint32(txid), readKeys, writeKeys, ethTxs[i])
		txs = append(txs, tx)
	}

	return NewBlock(blockID, txs)
}

func (tg *TxGenerator) GenerateWorkload(blockTxs []types.Transactions) []*Block {
	blocks := make([]*Block, 0)
	blockNum := tg.txNum / tg.blockSize // 区块数目

	for i := 0; i < blockNum; i++ {
		block := tg.GenerateBlock(i, blockTxs[i]) // 生成区块，i为区块号
		blocks = append(blocks, block)            // 添加区块至 blocks
	}
	return blocks
}
