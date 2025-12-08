package replay_geth

import (
	"fmt"

	"Janus/ethereum/config"
	"Janus/ethereum/core"
	"Janus/ethereum/database"
	"Janus/ethereum/ethdb"
	"Janus/ethereum/replay/replay_config"
)

func newProcessor() (*core.StateProcessor, ethdb.Database, error) {
	frdb, err := database.OpenDatabaseWithFreezer(database.DefaultPebbleConfig, database.DefaultRawConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("OpenDatabaseWithFreezer err:" + err.Error())
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

func ReplayGeth() {
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
		fmt.Println(block.Number(), block.Transactions())
		_, err = processor.Process(block, alldbForState.StateDB, config.DefaultVmConfig)
		if err != nil {
			fmt.Println(err)
		}

		// Commit all cached state changes into underlying memory database.
		root, _, err := alldbForState.StateDB.CommitWithUpdate(block.NumberU64(), config.MainnetChainConfig.IsEIP158(block.Number()), config.MainnetChainConfig.IsCancun(block.Number(), block.Time()))
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("blockNumber="+blockNumber.String()+"\t block state root:", block.Root())
		fmt.Println("blockNumber="+blockNumber.String()+"\t process state root:", root)
		parentStateRoot = root
	}

	//fmt.Printf("Hello and welcome, %s!\n", s)
	//
	//for i := 1; i <= 5; i++ {
	//	fmt.Println("i =", 100/i)
	//}
}
