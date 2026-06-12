package database

import (
	"path/filepath"
	"runtime"

	"Janus/ethereum/core/rawdb"
	"Janus/ethereum/triedb"
	"Janus/ethereum/triedb/hashdb"
	"Janus/ethereum/triedb/pathdb"

	"github.com/ethereum/go-ethereum/core"
)

type PebbleConfig struct {
	File      string
	Cache     int // MB
	Handles   int
	Namespace string
	Readonly  bool
}

type RawConfig struct {
	Ancient          string // ancients directory
	Era              string // era files directory
	MetricsNamespace string // prefix added to freezer metric names
	ReadOnly         bool
}

type StateDBConfig struct {
	Path    string
	Cache   int
	Handles int
}

var path string
var DefaultPebbleConfig *PebbleConfig
var DefaultRawConfig *RawConfig
var DefaultStateDBConfig *StateDBConfig
var SmallBankStateDBConfig *StateDBConfig

func init() {
	projectRoot := defaultProjectRoot()
	if runtime.GOOS == "darwin" {
		path = "/Volumes/ETH_DATA/ethereum/geth/chaindata"
	} else {
		path = "/data/ethereum/execution/geth/chaindata"
		//path = "/root/ethereum/execution/geth/chaindata"
	}
	DefaultPebbleConfig = &PebbleConfig{
		File:      path,
		Cache:     21462, // 如果内存较小，请修改
		Handles:   524288,
		Namespace: "eth/db/chaindata/",
		Readonly:  true,
	}

	DefaultRawConfig = &RawConfig{
		Ancient:          filepath.Join(path, "ancient"),
		Era:              "",
		MetricsNamespace: "eth/db/chaindata/",
		ReadOnly:         true,
	}

	DefaultStateDBConfig = &StateDBConfig{
		Path:    "/data/ethereum/state_snapshot",
		Cache:   32768,
		Handles: 32768,
	}
	SmallBankStateDBConfig = &StateDBConfig{
		Path:    filepath.Join(projectRoot, "data", "smallbank_database"),
		Cache:   0,
		Handles: 0,
	}

}

func defaultProjectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(filepath.Dir(filename)))
}

func trieDBConfig(blockChainConfig *core.BlockChainConfig, isVerkle bool) *triedb.Config {
	config := &triedb.Config{
		Preimages: blockChainConfig.Preimages,
		IsVerkle:  isVerkle,
	}
	if blockChainConfig.StateScheme == rawdb.HashScheme {
		config.HashDB = &hashdb.Config{
			CleanCacheSize: blockChainConfig.TrieCleanLimit * 1024 * 1024,
		}
	}
	if blockChainConfig.StateScheme == rawdb.PathScheme {
		config.PathDB = &pathdb.Config{
			StateHistory:        blockChainConfig.StateHistory,
			EnableStateIndexing: blockChainConfig.ArchiveMode,
			TrieCleanSize:       blockChainConfig.TrieCleanLimit * 1024 * 1024,
			StateCleanSize:      blockChainConfig.SnapshotLimit * 1024 * 1024,
			JournalDirectory:    blockChainConfig.TrieJournalDirectory,

			// TODO(rjl493456442): The write buffer represents the memory limit used
			// for flushing both trie data and state data to disk. The config name
			// should be updated to eliminate the confusion.
			WriteBufferSize: blockChainConfig.TrieDirtyLimit * 1024 * 1024,
			NoAsyncFlush:    blockChainConfig.TrieNoAsyncFlush,
		}
	}
	return config
}
