package main

import (
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/database"
	lvm "chukonu/levm"
	"chukonu/monitor"
	"chukonu/stmcore/blockstm"
	"chukonu/tools"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/params"
	"github.com/xuri/excelize/v2"
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

func writeTPSResultToExcel(filename string, baselines []string, tpss []float64) error {
	fmt.Println(filename)
	os.Remove(filename)
	// 1. 提取目录并创建（如果不存在）
	dir := filepath.Dir(filename)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Printf("创建目录失败: %v\n", err)
			return err
		}
	}

	f := excelize.NewFile()

	sheet := "TPS"
	_, err := f.NewSheet(sheet)
	if err != nil {
		return err
	}

	// 标题行
	err = f.SetCellValue(sheet, "A1", "Baseline")
	if err != nil {
		return err
	}
	err = f.SetCellValue(sheet, "B1", "TPS")
	if err != nil {
		return err
	}

	for i := 0; i < len(baselines) && i < len(tpss); i++ {
		row := i + 2
		err = f.SetCellValue(sheet, fmt.Sprintf("A%d", row), baselines[i])
		if err != nil {
			return err
		}
		err = f.SetCellValue(sheet, fmt.Sprintf("B%d", row), tpss[i])
		if err != nil {
			return err
		}
	}

	// 删除默认Sheet1
	err = f.DeleteSheet("Sheet1")
	if err != nil {
		return err
	}

	return f.SaveAs(filename)
}

func run(baseline, baseFileName string, tpss *[]float64, signalChan chan struct{}, signalWg *sync.WaitGroup, blockTxs []types.Transactions, alldb *database.AllDBForState, stmStateDB *state.StmStateDB) {
	monitorFilePath := filepath.Join(tools.MonitorBasePath, baseline+"/"+baseFileName)
	go monitor.MonitorMetrics(500*time.Millisecond, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
	*tpss = append(*tpss, blockstm.Run(blockTxs, alldb, stmStateDB))
}

func main() {
	baseline := flag.String("baseline", "blockstm",
		"baseline: (default blockstm)\n"+
			"\t\"blockstm\" is run all blockstm\n")
	threadNumber := flag.String("thread", "8", "thread number(default 8)")
	skew := flag.String("skew", "0.5", "thread number(default 0.5)")
	txNumber := flag.String("txNum", "100000", "thread number(default 6000)")
	flag.Parse()

	fmt.Println("baseline: ", *baseline, "\tthreadNumber: ", *threadNumber,
		"\tskew: ", *skew, "\ttxNumber: ", *txNumber)

	if *baseline != "blockstm" {
		fmt.Println("baseline is invalid")
		return
	}

	tools.AllThreadNum, _ = strconv.Atoi(*threadNumber)
	tools.Skew, _ = strconv.ParseFloat(*skew, 64)
	tools.TxNum, _ = strconv.Atoi(*txNumber)

	runtime.GOMAXPROCS(tools.AllThreadNum + 2)
	var (
		baseFileName           = "thread(" + strconv.Itoa(tools.AllThreadNum) + ")_skew(" + fmt.Sprintf("%f", tools.Skew) + ").xlsx"
		tpss         []float64 = make([]float64, 0)
		baselines              = []string{"blockstm"}
	)
	//tools.Skew = skewFromBias(tools.Skew)

	blockNum := tools.TxNum / tools.BlockSize
	blockTxs := make([]types.Transactions, 0) // 每个block的交易集合
	for i := 0; i < blockNum; i++ {
		txsLen := tools.BlockSize
		// Step 1: 生成地址
		addresses := tools.GenerateAddresses(1, int(float64(txsLen)*tools.AddressNumberRate))
		fmt.Printf("生成地址数量: %d\n", len(addresses))

		// Step 2: 生成交易（Zipf 控制冲突率）
		ethTxs := tools.GenerateSmallBankTxs(addresses, int(float64(txsLen)*tools.IoTxCountRate), int(float64(txsLen)*tools.CompetingTxCountRate),
			tools.FibonacciN, tools.RecursiveCalculateFibonacci, tools.Skew)
		fmt.Printf("生成交易数量: %d\n", len(ethTxs)) // 生成以太坊交易
		blockTxs = append(blockTxs, ethTxs)
	}

	fmt.Println("正在预读取状态...")
	levm := lvm.New(stateConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())
	stmStateDB, err := state.NewStmStateDB(tools.StateRoot, levm.AllDB().TrieDB, nil, levm.AllDB().StateDB)
	tools.PanicError("NewStmStateDB ", err)

	lvm.PreReadState(blockTxs, levm, stmStateDB)
	defer levm.AllDB().Close()

	if *baseline != "all" {
		baselines = []string{*baseline}
	}

	//tools.PreReadState = true
	for _, bl := range baselines {
		signalChan := make(chan struct{})
		signalWg := new(sync.WaitGroup)
		signalWg.Add(1)
		run(bl, baseFileName, &tpss, signalChan, signalWg, blockTxs, levm.AllDB(), stmStateDB)
		signalChan <- struct{}{}
		close(signalChan)
		signalWg.Wait()
	}
	err = writeTPSResultToExcel(filepath.Join(tools.MonitorBasePath, "tps"+"/blockstm_"+baseFileName), baselines, tpss)
	if err != nil {
		fmt.Println(err)
	}
}
