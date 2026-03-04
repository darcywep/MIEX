package blockstm

import (
	"chukonu/core"
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/ethdb"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
)

func blockSTM(txs types.BlockStmTxs, config *params.ChainConfig, chainDb ethdb.Database, stateDB *state.StmStateDB, threads int) (*common.Hash, error) {
	var wg sync.WaitGroup
	stmProcessor := core.NewScheduler(nil, txs, config, chainDb)
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go func() {
			defer wg.Done()
			newThread := core.NewThread(stmProcessor, stateDB, chainDb)
			newThread.Run()
		}()
	}
	wg.Wait()
	//stateDB.FinaliseMVMemory()
	root := stateDB.IntermediateRoot(true)
	fmt.Println("re-execution ", stmProcessor.GetExecutionCounter())
	return &root, nil
}
