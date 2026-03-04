package types

import (
	"github.com/ethereum/go-ethereum/common"
	"math/big"
)

type BlockStmTxs []*BlockStmTx

// BlockStmTx bind tx state db for tx
type BlockStmTx struct {
	Tx      *Transaction
	Block   *Block
	Rewards *map[common.Address]*big.Int
}

func NewBlockStmTx(tx *Transaction, block *Block, rewards *map[common.Address]*big.Int) *BlockStmTx {
	return &BlockStmTx{
		Tx:      tx,
		Block:   block,
		Rewards: rewards,
	}
}
