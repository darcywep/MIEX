package janus

import (
	janusConfig "janus-geth-1165/config"
	"janus-geth-1165/ethereum/core/types"
)

// Batch 表示一个执行批次
type Batch struct {
	ID          int
	LongTxs     []*janusTransaction // 长交易（计算型）
	ShortTxs    []*janusTransaction // 短交易（IO型）
	AllTxs      []*janusTransaction // 所有交易（按原始顺序）
	WatermarkID int                 // 水位线位置的交易ID
}

// BatchGenerator 批次生成器
type BatchGenerator struct {
	cpuCores int
	alpha    float64
	beta     float64
}

// NewBatchGenerator 创建批次生成器
func NewBatchGenerator(threadNumber int) *BatchGenerator {
	return &BatchGenerator{
		cpuCores: threadNumber,
		alpha:    janusConfig.WaterMarkAlpha,
		beta:     janusConfig.WaterMarkBeta,
	}
}

// GenerateBatches 根据交易序列生成批次
// 输入：区块中的原始交易序列（按共识顺序）
// 输出：多个批次 Batch 1, Batch 2, ... Batch n
func (bg *BatchGenerator) GenerateBatches(txs []*types.Transaction) ([]*Batch, []*janusTransaction) {
	if len(txs) == 0 {
		return []*Batch{}, []*janusTransaction{}
	}

	batches := make([]*Batch, 0)
	batchID := 0

	// 当前批次
	currentBatch := &Batch{
		ID:       batchID,
		LongTxs:  make([]*janusTransaction, 0),
		ShortTxs: make([]*janusTransaction, 0),
		AllTxs:   make([]*janusTransaction, 0),
	}
	jtxs := make([]*janusTransaction, len(txs))

	longTxCount := 0  // 当前批次长交易数量
	totalTxCount := 0 // 当前批次交易总数

	// 计算批次结束条件的阈值
	longTxThreshold := int(float64(bg.cpuCores) * bg.alpha)
	totalTxThreshold := int(float64(bg.cpuCores) * bg.beta)

	// 遍历所有交易
	for txIdx, tx := range txs {

		// 判断交易类型
		isLongTx := tx.TxType == janusConfig.LongTx
		jtx := &janusTransaction{
			Tx:          tx,
			IsLongTx:    isLongTx,
			EarlyAbort:  false,
			IsRuned:     false,
			OriginalIdx: txIdx,
		}
		jtxs[txIdx] = jtx
		// 添加交易到当前批次
		currentBatch.AllTxs = append(currentBatch.AllTxs, jtx)
		if isLongTx {
			currentBatch.LongTxs = append(currentBatch.LongTxs, jtx)
			longTxCount++
		} else {
			currentBatch.ShortTxs = append(currentBatch.ShortTxs, jtx)
		}
		totalTxCount++

		// 检查是否满足批次结束条件
		shouldEndBatch := false

		// 条件1：长交易数量达到阈值
		if longTxCount >= longTxThreshold {
			shouldEndBatch = true
			currentBatch.WatermarkID = txIdx
		} else if totalTxCount >= totalTxThreshold {
			// 条件2：总交易数达到阈值
			shouldEndBatch = true
			currentBatch.WatermarkID = txIdx
		}

		// 如果满足结束条件且不是最后一笔交易，则开始新批次
		if shouldEndBatch && txIdx < len(txs)-1 {
			batches = append(batches, currentBatch)

			// 创建新批次
			batchID++
			currentBatch = &Batch{
				ID:       batchID,
				LongTxs:  make([]*janusTransaction, 0),
				ShortTxs: make([]*janusTransaction, 0),
				AllTxs:   make([]*janusTransaction, 0),
			}
			longTxCount = 0
			totalTxCount = 0
		}
	}

	// 添加最后一个批次（如果有交易）
	if len(currentBatch.AllTxs) > 0 {
		currentBatch.WatermarkID = len(txs) - 1
		batches = append(batches, currentBatch)
	}

	return batches, jtxs
}
