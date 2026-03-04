package vminterface

import (
	"chukonu/core"
	"chukonu/core/vm"
	"crypto/rand"
	"crypto/sha256"
	"math/big"
	"time"

	"chukonu/ethdb"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// NewVMContext will construct a new EVM Context with default values.
// TODO: include gas price variable in params
func NewVMContext(coinbase common.Address, blockNum *big.Int, chainContext ChainContext, chainDb ethdb.Database) vm.BlockContext {
	// 生成 32 字节随机数据
	var randomBytes [32]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		panic(err)
	}
	random := crypto.Keccak256Hash(randomBytes[:]) // 使用 Keccak256 生成哈希（等价于 Solidity 的 keccak256）

	return vm.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    Transfer,
		GetHash:     core.GetHashFn(chainContext.GetHeader(sha256.Sum256([]byte("")), 0), chainDb),
		Coinbase:    coinbase,
		BlockNumber: new(big.Int).Set(blockNum),
		Time:        big.NewInt(time.Now().Unix()).Uint64(),
		Difficulty:  new(big.Int).SetInt64(1),
		BaseFee:     big.NewInt(1),
		GasLimit:    uint64(1000000),
		Random:      &random,
	}
}

// CanTransfer checks whether there are enough funds in the address' account to make a transfer.
// This does not take the necessary gas in to account to make the transfer valid.
func CanTransfer(db vm.StateDB, addr common.Address, amount *big.Int) bool {
	return db.GetBalance(addr).Cmp(amount) >= 0
}

// Transfer subtracts amount from sender and adds amount to recipient using the given Db
func Transfer(db vm.StateDB, sender, recipient common.Address, amount *big.Int) {
	db.SubBalance(sender, amount)
	db.AddBalance(recipient, amount)
}
