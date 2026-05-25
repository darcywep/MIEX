package replay_config

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

var (
	//RootBlockNumber   *big.Int = new(big.Int).SetInt64(21_000_000)
	//StartBlockNumber  *big.Int = new(big.Int).SetInt64(21_000_001)

	RootBlockNumber   *big.Int = new(big.Int).SetInt64(22_800_000)
	StartBlockNumber  *big.Int = new(big.Int).SetInt64(22_800_001)
	FinishBlockNumber *big.Int = new(big.Int).SetInt64(23_600_000)
	AddSpan           *big.Int = new(big.Int).SetInt64(1)
	ParentStateRoot   common.Hash
)

//var (
//	rootBlockNumber   *big.Int = new(big.Int).SetInt64(2380000)
//	startBlockNumber  *big.Int = new(big.Int).SetInt64(2380001)
//	finishBlockNumber *big.Int = new(big.Int).SetInt64(2380005)
//	addSpan           *big.Int = new(big.Int).SetInt64(1)
//	parentStateRoot   common.Hash
//)
