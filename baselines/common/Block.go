package common

import (
	"sync"
)

type Block struct {
	mu      sync.RWMutex
	BlockId int
	Txs     []*JanusTransaction
}

func NewBlock(blockId int, txs []*JanusTransaction) *Block {
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

func (b *Block) GetTxs() []*JanusTransaction {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.Txs
}
