package database

import (
	"fmt"
	"math/big"

	"Janus/ethereum/core/rawdb"
	"Janus/ethereum/core/types"
	"Janus/ethereum/ethdb"
	"Janus/ethereum/ethdb/pebble"

	"github.com/ethereum/go-ethereum/common"
)

// OpenDatabase is not ancient db
func OpenDatabase(pebbleConfig *PebbleConfig) (ethdb.Database, error) {
	fmt.Println("Opening database", pebbleConfig.File)
	db, err := pebble.New(pebbleConfig.File, pebbleConfig.Cache, pebbleConfig.Handles, pebbleConfig.Namespace, pebbleConfig.Readonly)
	if err != nil {
		return nil, err
	}

	return rawdb.NewDatabase(db), nil
}

func OpenDatabaseWithFreezer(pebbleConfig *PebbleConfig, rawConfig *RawConfig) (ethdb.Database, error) {
	db, err := pebble.New(pebbleConfig.File, pebbleConfig.Cache, pebbleConfig.Handles, pebbleConfig.Namespace, pebbleConfig.Readonly)
	if err != nil {
		return nil, err
	}

	frdb, err := rawdb.Open(db, rawdb.OpenOptions{
		Ancient:          rawConfig.Ancient,
		Era:              rawConfig.Era,
		MetricsNamespace: rawConfig.MetricsNamespace,
		ReadOnly:         rawConfig.ReadOnly,
	})
	return frdb, err
}

func GetBlockByNumber(db ethdb.Database, number *big.Int) (*types.Block, error) {
	var (
		block *types.Block
		err   error
	)
	hash := rawdb.ReadCanonicalHash(db, number.Uint64()) // 获取区块hash
	if (hash != common.Hash{}) {
		block = rawdb.ReadBlock(db, hash, number.Uint64())
		if block == nil {
			err = fmt.Errorf("read block(" + number.String() + ") error! block is nil")
		}
	} else {
		err = fmt.Errorf("read block(" + number.String() + ") error! hash is nil")
	}
	return block, err
}

func GetHeaderByNumber(db ethdb.Database, number *big.Int) (*types.Header, error) {
	var (
		header *types.Header = nil
		err    error         = nil
	)

	hash := rawdb.ReadCanonicalHash(db, number.Uint64()) // 创建StateDB
	if (hash != common.Hash{}) {
		if h := rawdb.ReadHeader(db, hash, number.Uint64()); h != nil {
			header = h
		} else {
			err = fmt.Errorf("create stateDB error! header is nil")
		}
	}
	return header, err
}
