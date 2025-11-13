package common

type TxGenerator struct {
	txNum     int
	blockSize int
	blocks    []*Block
}

func NewTxGenerator(txNum int, blockSize int) *TxGenerator {
	return &TxGenerator{txNum, blockSize, nil}
}

func (g *TxGenerator) GenerateTransaction(txid uint32, readKeys map[string]string, writeKeys map[string]string) *JanusTransaction {
	vertex := TransactionVertex{readKeys, writeKeys, nil}
	return NewJanusTransaction(txid, &vertex)
}

func (tg *TxGenerator) GenerateBlock(blockID int) *Block {

	txs := make([]*JanusTransaction, 0)

	for i := 0; i < tg.blockSize; i++ {

		txid := blockID*tg.blockSize + i + 1
		readKeys := make(map[string]string)
		writeKeys := make(map[string]string)

		tx := tg.GenerateTransaction(uint32(txid), readKeys, writeKeys)
		txs = append(txs, tx)
	}
	return NewBlock(blockID, txs)
}

func (tg *TxGenerator) GenerateWorkload() []*Block {
	blocks := make([]*Block, 0)
	blockNum := tg.txNum / tg.blockSize // 区块数目

	for i := 0; i < blockNum; i++ {
		block := tg.GenerateBlock(i)   // 生成区块，i为区块号
		blocks = append(blocks, block) // 添加区块至 blocks
	}
	return blocks
}
