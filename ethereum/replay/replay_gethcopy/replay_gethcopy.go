package replay_gethcopy

import (
	"fmt"
	"path/filepath"

	"Janus/ethereum/config"
	"Janus/ethereum/core"
	"Janus/ethereum/database"
	"Janus/ethereum/ethdb"
	"Janus/ethereum/replay/replay_config"
)

const chianDataPath = "/data/ethereum/state_snapshot/chaindata_2100/"

var (
	pebbleConfig = &database.PebbleConfig{
		File:      chianDataPath,
		Cache:     92160, // 如果内存较小，请修改
		Handles:   524288,
		Namespace: "eth/db/chaindata/",
		Readonly:  false,
	}
	rawConfig = &database.RawConfig{
		Ancient:          filepath.Join(pebbleConfig.File, "ancient"),
		Era:              "",
		MetricsNamespace: "eth/db/chaindata/",
		ReadOnly:         false,
	}
	stateDBConfig = &database.StateDBConfig{
		Path:    "/data/ethereum/state_snapshot",
		Cache:   32768,
		Handles: 32768,
	}
)

func newProcessor() (*core.StateProcessor, ethdb.Database, error) {
	frdb, err := database.OpenDatabase(pebbleConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("OpenDatabase err:" + err.Error())
	}
	engine, err := config.CreateConsensusEngine(config.MainnetChainConfig)
	if err != nil {
		_ = frdb.Close()
		return nil, nil, fmt.Errorf("CreateConsensusEngine err:" + err.Error())
	}

	bcHc, err := core.NewHeaderChain(frdb, config.MainnetChainConfig, engine)
	processor := core.NewStateProcessor(bcHc)
	return processor, frdb, nil
}

func ReplayCopy() {
	processor, frdb, err := newProcessor()
	if err != nil {
		panic(err)
		return
	}
	defer frdb.Close()
	blockPre, err := database.GetBlockByNumber(frdb, replay_config.RootBlockNumber)
	if err != nil {
		panic(err)
		return
	}
	var parentStateRoot = blockPre.Root()
	alldbForState, err := database.NewAllDBForState(database.DefaultStateDBConfig, blockPre.Number(), blockPre.Root(), false, false)
	defer alldbForState.Close()

	for blockNumber := replay_config.StartBlockNumber; blockNumber.Cmp(replay_config.FinishBlockNumber) == -1; blockNumber = blockNumber.Add(blockNumber, replay_config.AddSpan) {
		err := alldbForState.UpdateStateDB(parentStateRoot)
		if err != nil {
			panic(err)
			return
		}
		block, err := database.GetBlockByNumber(frdb, blockNumber)
		if err != nil {
			panic(err)
			return
		}
		_, err = processor.Process(block, alldbForState.StateDB, config.DefaultVmConfig)
		if err != nil {
			fmt.Println(err)
		}

		// Commit all cached state changes into underlying memory database.
		root, _, err := alldbForState.StateDB.CommitWithUpdate(block.NumberU64(), config.MainnetChainConfig.IsEIP158(block.Number()), config.MainnetChainConfig.IsCancun(block.Number(), block.Time()))
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("blockNumber="+blockNumber.String()+"\t process state root:", block.Root())
		fmt.Println("blockNumber="+blockNumber.String()+"\t block state root:", root)
		parentStateRoot = root
	}
}
