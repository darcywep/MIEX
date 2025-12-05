package replay_gethcopy

import (
	"Janus/ethereum/config"
	"Janus/ethereum/core/vm"
	"Janus/ethereum/database"
	"Janus/ethereum/replay/replay_config"
	"fmt"
)

func ReplayWithRecordOpCodeTiming() {
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

	vm.InitInstructionTimer(vm.TimingDataFile) // Initialize opcode timing recording

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
		statedbCopy := alldbForState.StateDB.Copy() // Create a copy of the StateDB for this block

		vm.TimingEnabled = false
		_, err = processor.Process(block, alldbForState.StateDB, config.DefaultVmConfig)
		if err != nil {
			fmt.Println(err)
		}

		vm.TimingEnabled = true
		_, err = processor.Process(block, statedbCopy, config.DefaultVmConfig) // 第二次再记录，避免过多的IO干扰
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
