package vminterface

import (
	"Janus/ethereum/config"
	"Janus/ethereum/consensus"
	"Janus/ethereum/core/types"
	"Janus/tools"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
)

// ChainContext needed by the evm
type ChainContext struct {
	coinbase common.Address
	header   *types.Header
}

// NewChainContext constructs a new context needed by EVMContext
func NewChainContext(coinbase common.Address) *ChainContext {
	return &ChainContext{
		coinbase: coinbase,
		header: &types.Header{
			Coinbase:   coinbase,
			Difficulty: big.NewInt(1),
			Number:     big.NewInt(0),
			GasLimit:   tools.Uint64,
			GasUsed:    uint64(0),
			Time:       uint64(time.Now().Unix()),
			Extra:      nil,
		},
	}
}

func (c *ChainContext) Config() *params.ChainConfig {
	return config.MainnetChainConfig
}

// CurrentHeader retrieves the current header from the local chain.
func (c *ChainContext) CurrentHeader() *types.Header {
	return c.header
}

// GetHeader retrieves a block header from the database by hash and number.
func (c *ChainContext) GetHeader(hash common.Hash, number uint64) *types.Header {
	return c.header
}

// GetHeaderByNumber retrieves a block header from the database by number.
func (c *ChainContext) GetHeaderByNumber(number uint64) *types.Header {
	return c.header
}

// GetHeaderByHash retrieves a block header from the database by its hash.
func (c *ChainContext) GetHeaderByHash(hash common.Hash) *types.Header {
	return c.header
}

// Engine is only here to satisfy the chaincontext interface
func (c *ChainContext) Engine() consensus.Engine {
	return nil
}
