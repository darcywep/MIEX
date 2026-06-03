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
	txs := make([]*BasicTransaction, 0, len(ethTxs))

	for i := 0; i < len(ethTxs); i++ {
		txid := blockID*tg.blockSize + i + 1
		readKeys := make(map[string]string)
		writeKeys := make(map[string]string)

		tx := tg.GenerateTransaction(uint32(txid), blockID, i, readKeys, writeKeys, ethTxs[i])
		txs = append(txs, tx)
	}

	return NewBlock(blockID, txs)
}

func (tg *TxGenerator) GenerateWorkload(blockTxs []types.Transactions) []*Block {
	blocks := make([]*Block, 0, len(blockTxs))
	txid := uint32(1)

	for blockID, ethTxs := range blockTxs {
		txs := make([]*BasicTransaction, 0, len(ethTxs))
		for txIndex, ethTx := range ethTxs {
			readKeys := make(map[string]string)
			writeKeys := make(map[string]string)
			tx := tg.GenerateTransaction(txid, blockID, txIndex, readKeys, writeKeys, ethTx)
			txs = append(txs, tx)
			txid++
		}
		blocks = append(blocks, NewBlock(blockID, txs))
	}
	return blocks
}

func (tg *TxGenerator) GenerateWorkloadForHarmonyAbort(blockTxs []types.Transactions) []*Block {
	return tg.GenerateWorkload(blockTxs)
}
