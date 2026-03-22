package common

import (
	"Janus/ethereum/core/types"
)

type TxGenerator struct {
	txNum     int
	blockSize int
	blocks    []*Block
}

func NewTxGenerator(txNum int, blockSize int) *TxGenerator {
	return &TxGenerator{txNum, blockSize, nil}
}

func (g *TxGenerator) GenerateTransaction(txid uint32, originalBlockID, originalTxID int, readKeys map[string]string, writeKeys map[string]string, ethTx *types.Transaction) *BasicTransaction {
	vertex := TransactionVertex{readKeys, writeKeys, nil}
	return NewBasicTransaction(txid, originalBlockID, originalTxID, &vertex, ethTx)
}

func (tg *TxGenerator) GenerateBlock(blockID int, ethTxs types.Transactions) *Block { // 生成的区块中包含EVM可执行交易
	txs := make([]*BasicTransaction, 0)

	for i := 0; i < tg.blockSize; i++ {
		txid := blockID*tg.blockSize + i + 1
		readKeys := make(map[string]string)
		writeKeys := make(map[string]string)

		tx := tg.GenerateTransaction(uint32(txid), blockID, i, readKeys, writeKeys, ethTxs[i])
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

func (tg *TxGenerator) GenerateWorkloadForHarmonyAbort(blockTxs []types.Transactions) []*Block {
	blockNum := len(blockTxs)
	blocks := make([]*Block, 0, blockNum)

	txid := 1 // 全局交易ID，从1开始递增
	for i := 0; i < blockNum; i++ {
		txsLen := len(blockTxs[i])
		txs := make([]*BasicTransaction, 0, txsLen)

		for j := 0; j < txsLen; j++ {
			readKeys := make(map[string]string)
			writeKeys := make(map[string]string)

			tx := tg.GenerateTransaction(uint32(txid), i, j, readKeys, writeKeys, blockTxs[i][j])
			txs = append(txs, tx)
			txid++ // 递增全局交易ID
		}

		blocks = append(blocks, NewBlock(i, txs)) // 添加区块至 blocks
	}
	return blocks
}
