package database

import "runtime"

type RawConfig struct {
	Path      string
	Cache     int
	Handles   int
	Ancient   string
	Namespace string
	ReadOnly  bool
}

func defaultRawConfig() *RawConfig {
	if runtime.GOOS == "darwin" { // MacOS
		return &RawConfig{
			Path:      "/Users/darcywep/Projects/ethereum/ethereumdata/copchaincopy",
			Cache:     2048,
			Handles:   5120,
			Ancient:   "/Users/darcywep/Projects/ethereum/ethereumdata/copchaincopy/ancient",
			Namespace: "eth/db/chaindata/",
			ReadOnly:  false,
		}
	} else {
		return &RawConfig{
			//Path: "/home/fuzh/chukonu/data/ethereumdata/copchaincopy",
			Path:    "/root/chukonu/ethereumdata/copchaincopy",
			Cache:   2048,
			Handles: 5120,
			//Ancient: "/home/fuzh/chukonu/data/ethereumdata/copchaincopy/ancient",
			Ancient:   "/root/chukonu/ethereumdata/copchaincopy/ancient",
			Namespace: "eth/db/chaindata/",
			ReadOnly:  false,
		}
	}
}

type StateDBConfig struct {
	Cache     int
	Journal   string
	Preimages bool
}

func defaultStateDBConfig() *StateDBConfig {
	if runtime.GOOS == "darwin" { // MacOS
		return &StateDBConfig{
			Cache: 614,
			//Journal:   "/Users/darcywep/Projects/ethereum/execution/geth/triecache",
			Journal:   "/Users/darcywep/Projects/ethereum/ethereumdata/triecache",
			Preimages: false,
		}
	} else {
		return &StateDBConfig{
			Cache: 614,
			//Journal: "/home/fuzh/chukonu/data/ethereumdata/triecache",
			Journal:   "/root/chukonu/ethereumdata/triecache",
			Preimages: false,
		}
	}
}

type StateDBConfig1165 struct { // Geth 1.16.5
	Path          string
	Cache         int
	Handles       int
	TrieCache     int
	TriePreimages bool
}
