package database

import (
	"fmt"
	"janus-geth-1115-blockstm/core/rawdb"
	"janus-geth-1115-blockstm/core/state"
	"janus-geth-1115-blockstm/core/state/snapshot"
	"janus-geth-1115-blockstm/core/types"
	"janus-geth-1115-blockstm/ethdb"
	"janus-geth-1115-blockstm/ethdb/leveldb"
	"janus-geth-1115-blockstm/trie"
	"math/big"
	"path/filepath"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

func NewStateCache(db ethdb.Database) state.Database {
	config := defaultStateDBConfig()
	return state.NewDatabaseWithConfig(db, &trie.Config{
		Cache:     config.Cache,
		Journal:   config.Journal,
		Preimages: config.Preimages,
	})
}

func NewSnap(db ethdb.Database, stateCache state.Database, header *types.Header) *snapshot.Tree {
	var recover bool

	if layer := rawdb.ReadSnapshotRecoveryNumber(db); layer != nil && *layer > header.Number.Uint64() {
		log.Warn("Enabling snapshot recovery", "chainhead", header.Number.Uint64(), "diskbase", *layer)
		recover = true
	}
	snapconfig := snapshot.Config{
		CacheSize:  256,
		Recovery:   recover,
		NoBuild:    true,
		AsyncBuild: false,
	}

	snaps, _ := snapshot.New(snapconfig, db, stateCache.TrieDB(), header.Root)
	return snaps
}

func NewStateDB(header *types.Header, stateCache state.Database, snaps *snapshot.Tree) *state.StateDB {
	stateDb, err := state.New(header.Root, stateCache, snaps)
	if err != nil {
		fmt.Println(stateDb, "New StateDB Error", err)
		return nil
	}
	return stateDb
}

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

type AllDBForState struct {
	DiskDB    ethdb.Database
	TrieDB    state.Database
	StateDB   *state.StateDB
	stateRoot common.Hash
}

func NewAllDBForState(stateConfig *StateDBConfig1165, blockNumber *big.Int, stateRoot common.Hash, isVerkle, readonly bool) (*AllDBForState, error) {
	diskPath := filepath.Join(stateConfig.Path, "snapshot_"+blockNumber.String())
	// 打开/创建新的链数据库
	levelDB, err := leveldb.New(diskPath, stateConfig.Cache, stateConfig.Handles, "state_snapshot", readonly)
	if err != nil {
		return nil, fmt.Errorf("open leveldb error: " + err.Error())
	}

	diskdb := rawdb.NewDatabase(levelDB)
	triedb := state.NewDatabaseWithConfig(diskdb, &trie.Config{
		Cache:     stateConfig.Cache,
		Journal:   filepath.Join(stateConfig.Path, "triecache"),
		Preimages: stateConfig.TriePreimages,
	})

	statedb, err := state.New(stateRoot, triedb, nil)
	if err != nil {
		return nil, fmt.Errorf("create state db: " + err.Error())
	}
	return &AllDBForState{
		DiskDB:    diskdb,
		TrieDB:    triedb,
		StateDB:   statedb,
		stateRoot: stateRoot,
	}, nil
}

func (a *AllDBForState) UpdateStateDB(stateRoot common.Hash) error {
	if stateRoot == a.stateRoot {
		return nil
	}
	statedb, err := state.New(stateRoot, a.TrieDB, nil)
	if err != nil {
		return fmt.Errorf("update state db: " + err.Error())
	}
	a.StateDB = statedb
	return nil
}

func (a *AllDBForState) Copy() *AllDBForState {
	return &AllDBForState{
		DiskDB:    a.DiskDB,
		TrieDB:    a.TrieDB,
		StateDB:   a.StateDB.Copy(),
		stateRoot: a.stateRoot,
	}
}

func (a *AllDBForState) Close() {
	_ = a.DiskDB.Close()
}
