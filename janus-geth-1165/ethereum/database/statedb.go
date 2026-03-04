package database

import (
	"fmt"
	"math/big"
	"path/filepath"

	"janus-geth-1165/ethereum/core/rawdb"
	"janus-geth-1165/ethereum/core/state"
	"janus-geth-1165/ethereum/ethdb/leveldb"
	"janus-geth-1165/ethereum/triedb"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
)

type AllDBForState struct {
	DiskDB            *leveldb.Database
	TrieDB            *triedb.Database
	BlockChainStateDB *state.CachingDB
	StateDB           *state.StateDB
	StateRoot         common.Hash
}

func NewAllDBForState(stateConfig *StateDBConfig, blockNumber *big.Int, stateRoot common.Hash, isVerkle, readonly bool) (*AllDBForState, error) {
	diskPath := filepath.Join(stateConfig.Path, "snapshot_"+blockNumber.String())
	// 打开/创建新的链数据库
	levelDB, err := leveldb.New(diskPath, stateConfig.Cache, stateConfig.Handles, "state_snapshot", readonly)
	if err != nil {
		return nil, fmt.Errorf("open leveldb error: " + err.Error())
	}

	triedb := triedb.NewDatabase(rawdb.NewDatabase(levelDB), trieDBConfig(core.DefaultConfig(), isVerkle))

	bcStateDB := state.NewDatabase(triedb, nil)
	statedb, err := state.New(stateRoot, bcStateDB)
	if err != nil {
		return nil, fmt.Errorf("create state db: " + err.Error())
	}
	return &AllDBForState{
		DiskDB:            levelDB,
		TrieDB:            triedb,
		BlockChainStateDB: bcStateDB,
		StateDB:           statedb,
		StateRoot:         stateRoot,
	}, nil
}

func (a *AllDBForState) UpdateStateDB(stateRoot common.Hash) error {
	if stateRoot == a.StateRoot {
		return nil
	}
	statedb, err := state.New(stateRoot, a.BlockChainStateDB)
	if err != nil {
		return fmt.Errorf("create state db: " + err.Error())
	}
	a.StateDB = statedb
	return nil
}

func (a *AllDBForState) Close() {
	_ = a.TrieDB.Close()
	_ = a.DiskDB.Close()
}

//func NewSnap(db ethdb.Database, stateCache state.Database, header *types.Header) *snapshot.Tree {
//	var recover bool
//
//	if layer := rawdb.ReadSnapshotRecoveryNumber(db); layer != nil && *layer > header.Number.Uint64() {
//		log.Warn("Enabling snapshot recovery", "chainhead", header.Number.Uint64(), "diskbase", *layer)
//		recover = true
//	}
//	snapconfig := snapshot.Config{
//		CacheSize:  256,
//		Recovery:   recover,
//		NoBuild:    true,
//		AsyncBuild: false,
//	}
//
//	snaps, _ := snapshot.New(snapconfig, db, stateCache.TrieDB(), header.Root)
//	return snaps
//}

//func NewStateDB(header *types.Header, stateCache state.Database, snaps *snapshot.Tree) *state.StateDB {
//	stateDb, err := state.New(header.Root, stateCache, snaps)
//	if err != nil {
//		fmt.Println(stateDb, "New StateDB Error", err)
//		return nil
//	}
//	return stateDb
//}

//func NewStmStateDB(header *types.Header, stateCache state.Database, snaps *snapshot.Tree) *state.StmStateDB {
//	stateDb, err := state.NewStmStateDB(header.Root, stateCache, snaps)
//	if err != nil {
//		fmt.Println(stateDb, "New StateDB Error", err)
//		return nil
//	}
//	return stateDb
//}

//func NewStateDatabase(db ethdb.Database, number uint64, parent *types.Header) (*state.StateDB, error) {
//	var stateDB *state.StateDB = nil
//	var err error
//	hash := rawdb.ReadCanonicalHash(db, number) // 创建StateDB
//	if (hash != common.Hash{}) {
//		if header := rawdb.ReadHeader(db, hash, number); header != nil {
//			parent = header
//			stateDB = newStateCache(db, header)
//		} else {
//			err = fmt.Errorf("create stateDB error! header is nil")
//		}
//	} else {
//		err = fmt.Errorf("create stateDB error! header is nil")
//	}
//	return stateDB, err
//}
