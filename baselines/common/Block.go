package common

import (
	"sync"
)

type Block struct {
	mu sync.RWMutex

	BlockId       int
	Txs           []*Transaction
	TxInfo        []*HyperVertex
	InvertedIndex map[string]*RWSets
	RWIndex       map[*Vertex]map[*Vertex]bool
	ConflictIndex map[*Vertex]map[*Vertex]bool
	RBIndex       map[string]map[*Vertex]bool
	TotalCost     int
}

func NewBlock(blockId int, txs []*Transaction, txInfos []*HyperVertex,
	invertedIndex map[string]*RWSets,
	rwIndex map[*Vertex]map[*Vertex]bool,
	conflictIndex map[*Vertex]map[*Vertex]bool,
	rbIndex map[string]map[*Vertex]bool,
	totalCost int) *Block {

	return &Block{
		BlockId:       blockId,
		Txs:           txs,
		TxInfo:        txInfos,
		InvertedIndex: invertedIndex,
		RWIndex:       rwIndex,
		ConflictIndex: conflictIndex,
		RBIndex:       rbIndex,
		TotalCost:     totalCost,
	}
}

func (b *Block) GetBlockId() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.BlockId
}

func (b *Block) SetTxs(txs []*Transaction) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Txs = txs
}

func (b *Block) GetTxs() []*Transaction {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.Txs
}

func (b *Block) GetTxList() []*HyperVertex {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.TxInfo
}

func (b *Block) SetInvertedIndex(invertedIndex map[string]*RWSets) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.InvertedIndex = invertedIndex
}

func (b *Block) GetInvertedIndex() map[string]*RWSets {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make(map[string]*RWSets)
	for k, v := range b.InvertedIndex {
		result[k] = v
	}
	return result
}

func (b *Block) GetTotalCost() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.TotalCost
}
