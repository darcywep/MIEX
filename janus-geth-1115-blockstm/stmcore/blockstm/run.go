package blockstm

import (
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/database"
	"chukonu/tools"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/params"
)

func NewUint64(number uint64) *uint64 {
	return &number
}

var stateConfig *database.StateDBConfig1165
var newChainConfig *params.ChainConfig

func init() {
	stateConfig = &database.StateDBConfig1165{
		Path:          "/root/alldb/smallbank_database",
		Cache:         16000,
		Handles:       16000,
		TrieCache:     16000,
		TriePreimages: false,
	}
	newChainConfig = &params.ChainConfig{
		ChainID:                 big.NewInt(1),
		HomesteadBlock:          big.NewInt(0),
		DAOForkBlock:            big.NewInt(0),
		DAOForkSupport:          true,
		EIP150Block:             big.NewInt(0),
		EIP155Block:             big.NewInt(0),
		EIP158Block:             big.NewInt(0),
		ByzantiumBlock:          big.NewInt(0),
		ConstantinopleBlock:     big.NewInt(0),
		PetersburgBlock:         big.NewInt(0),
		IstanbulBlock:           big.NewInt(0),
		MuirGlacierBlock:        big.NewInt(0),
		BerlinBlock:             big.NewInt(0),
		LondonBlock:             big.NewInt(0),
		ArrowGlacierBlock:       big.NewInt(0),
		GrayGlacierBlock:        big.NewInt(0),
		TerminalTotalDifficulty: big.NewInt(0), // 58_750_000_000_000_000_000_000
		ShanghaiTime:            NewUint64(0),
		CancunTime:              NewUint64(0),
		PragueTime:              NewUint64(0),
		Ethash:                  new(params.EthashConfig),
	}
}

func Run(blockTxs []types.Transactions, alldb *database.AllDBForState, stateDB *state.StmStateDB) float64 {
	fmt.Println("=== SmallBank 测试框架启动 ===")

	blockNum := tools.TxNum / tools.BlockSize
	fromAddress := tools.GenerateAddress()
	var block = types.NewBlock(&types.Header{
		Coinbase:   fromAddress,
		Difficulty: big.NewInt(1),
		Number:     big.NewInt(0),
		GasLimit:   uint64(1e19),
		BaseFee:    big.NewInt(1),
		GasUsed:    uint64(0),
		Time:       uint64(time.Now().Unix()),
		Extra:      nil,
	}, nil, nil, nil, nil)
	//allDBForState := alldb.Copy()
	stmStateDB := stateDB.Copy()
	start2 := time.Now()
	for i := 0; i < blockNum; i++ {
		// Step 3: 模拟执行

		var (
			cknTxs     = make(types.BlockStmTxs, 0)
			cknTxIndex = 0
		)
		for _, tx := range blockTxs[i] {
			tx.Index = cknTxIndex
			cknTxs = append(cknTxs, types.NewBlockStmTx(tx, block, nil))
			cknTxIndex += 1
		}

		//stmStateDB, err := state.NewStmStateDB(tools.StateRoot, allDBForState.TrieDB, nil, allDBForState.StateDB) // 每个区块重新构建statedb以释放内存
		//if err != nil {
		//	fmt.Println("NewStmStateDB " + err.Error())
		//}

		//_, err := blockSTM(cknTxs, newChainConfig, allDBForState.DiskDB, stmStateDB, tools.AllThreadNum)
		_, err := blockSTM(cknTxs, newChainConfig, alldb.DiskDB, stmStateDB, tools.AllThreadNum)
		if err != nil {
			fmt.Println("blockSTM", err)
		}
		//root, err := allDBForState.StateDB.Commit(true)
		//if err != nil {
		//	fmt.Println("stateDB.Commit(true)", err)
		//}
		//err = allDBForState.StateDB.Database().TrieDB().Commit(root, false)
		//if err != nil {
		//	fmt.Println("TrieDB().Commit(root, false)", err)
		//}
		//allDBForState.Close()
	}
	end2 := time.Since(start2)
	tps := float64(tools.TxNum) / end2.Seconds()
	fmt.Println("BlockStm TPS:", tps)
	return tps
}
