package tools

import (
	janusConfig "Janus/config"
	"Janus/ethereum/core/types"
	"fmt"
	"math"
	"math/rand"
)

// TxTypeMisclassificationStats 记录一次长/短交易误判注入的结果。
type TxTypeMisclassificationStats struct {
	Rate             float64
	Seed             int64
	CandidateTxs     int
	MisclassifiedTxs int
	LongToShort      int
	ShortToLong      int
}

// ApplyTxTypeMisclassification 随机选取一部分交易，并只翻转它们的调度类型。
// 交易真实 TxType 不会被修改，因此执行代价和读写集仍由原始交易决定。
func ApplyTxTypeMisclassification(blockTxs []types.Transactions, rate float64, seed int64) (TxTypeMisclassificationStats, error) {
	stats := TxTypeMisclassificationStats{
		Rate: rate,
		Seed: seed,
	}
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate < 0 || rate > 1 {
		return stats, fmt.Errorf("tx type misclassification rate must be in [0, 1], got %f", rate)
	}

	candidates := make([]*types.Transaction, 0)
	for _, txs := range blockTxs {
		for _, tx := range txs {
			if tx == nil {
				continue
			}
			tx.ClearScheduleTxType()
			if tx.TxType == janusConfig.LongTx || tx.TxType == janusConfig.ShortTx {
				candidates = append(candidates, tx)
			}
		}
	}

	stats.CandidateTxs = len(candidates)
	if rate == 0 || len(candidates) == 0 {
		return stats, nil
	}

	target := int(math.Round(float64(len(candidates)) * rate))
	if target > len(candidates) {
		target = len(candidates)
	}
	if target == 0 {
		return stats, nil
	}

	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	for _, tx := range candidates[:target] {
		switch tx.TxType {
		case janusConfig.LongTx:
			tx.SetScheduleTxType(janusConfig.ShortTx)
			stats.LongToShort++
		case janusConfig.ShortTx:
			tx.SetScheduleTxType(janusConfig.LongTx)
			stats.ShortToLong++
		}
	}
	stats.MisclassifiedTxs = stats.LongToShort + stats.ShortToLong
	return stats, nil
}
