package common

import (
	"sync"
)

type Block struct {
	mu      sync.RWMutex
	BlockId int
	Txs     []*BasicTransaction
}

func NewBlock(blockId int, txs []*BasicTransaction) *Block {
	return &Block{
		BlockId: blockId,
		Txs:     txs,
	}
}

func (b *Block) GetBlockId() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.BlockId
}

func (b *Block) GetTxs() []*BasicTransaction {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.Txs
}
