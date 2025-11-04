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

type pebbleConfig struct {
	file      string
	cache     int // MB
	handles   int
	namespace string
	readonly  bool
}

type rawConfig struct {
	ancient          string // ancients directory
	era              string // era files directory
	metricsNamespace string // prefix added to freezer metric names
	readOnly         bool
}

type StateDBConfig struct {
	Path    string
	Cache   int
	Handles int
}

var path string
var defaultPebbleConfig *pebbleConfig
var defaultRawConfig *rawConfig
var DefaultStateDBConfig *StateDBConfig

func init() {
	if runtime.GOOS == "darwin" {
		path = "/Volumes/ETH_DATA/ethereum/geth/chaindata"
	} else {
		path = "/data/ethereum/execution/geth/chaindata"
		//path = "/root/ethereum/execution/geth/chaindata"
	}
	defaultPebbleConfig = &pebbleConfig{
		file:      path,
		cache:     21462, // 如果内存较小，请修改
		handles:   524288,
		namespace: "eth/db/chaindata/",
		readonly:  true,
	}

	defaultRawConfig = &rawConfig{
		ancient:          filepath.Join(path, "ancient"),
		era:              "",
		metricsNamespace: "eth/db/chaindata/",
		readOnly:         true,
	}

	DefaultStateDBConfig = &StateDBConfig{
		Path:    "/data/ethereum/state_snapshot",
		Cache:   32768,
		Handles: 32768,
	}

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
