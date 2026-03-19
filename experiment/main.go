package main

import (
	"Janus/baselines/optme/optme"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/config"
	"Janus/ethereum/core/types"
	"Janus/ethereum/core/vm"
	"Janus/ethereum/database"
	janusClassicAbort "Janus/januscore/janus_classic_abort"
	janusClassicDAG "Janus/januscore/janus_classic_dag"
	janus_calssic_occ "Janus/januscore/janus_classic_occ"
	"Janus/monitor"
	"Janus/tools"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	//"io"
	//"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"math/big"

	"github.com/ethereum/go-ethereum/params"
	"github.com/xuri/excelize/v2"
)

var stateConfig *database.StateDBConfig
var chainConfig *params.ChainConfig

var (
	threadNumber  = 8
	blockNumber   = 10   // 总的交易数量
	blockTxNumber = 2000 // 每个区块的交易数量
	skew          = 1.01

	addressNumberRate = 4 // 总共生成多少个地址 = blockTxSum * addressNumberRate

	// longTxCountRate + shortTxCountRate = 1
	longTxCountRate  = 0.5 // 长交易的比例
	shortTxCountRate = 0.5 // 短交易的比例

	waterMarkAlpha = 1.5 // 水位线参数 α
	waterMarkBeta  = 3.5 // 水位线参数 β

	fibonacciN                  = 10    // -1: 代表随机生成
	shortTxFibonacciLoopNumber  = 20    // 循环执行 fibonacciLoopNumber 次斐波那契计算
	longTxFibonacciLoopNumber   = 40    // 循环执行 fibonacciLoopNumber 次斐波那契计算
	recursiveCalculateFibonacci = false // 是否使用递归计算斐波那契
)

type InputData struct {
	Baseline     string
	ThreadNumber int
	BlockNumber  int
	BlockTxNum   int

	Skew float64

	LongTxCountRate  float64
	ShortTxCountRate float64

	WaterMarkAlpha float64
	WaterMarkBeta  float64

	FibonacciN                  int
	FibonacciLoopNum            int
	RecursiveCalculateFibonacci bool

	Txs [][][]int
}

func init() {
	stateConfig = &database.StateDBConfig{
		Path:    "/root/alldb/smallbank_database",
		Cache:   16000,
		Handles: 16000,
	}
	chainConfig = config.TestChainConfig
}

func runTool(binary string, input InputData) {

	data, err := json.Marshal(input)
	if err != nil {
		panic(err)
	}

	cmd := exec.Command(binary)

	cmd.Stdin = bytes.NewReader(data)

	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println("error:", err)
	}

	fmt.Println("------", binary, "------")
	fmt.Println(string(out))
}

func runProject(path string, input InputData) {

	data, _ := json.Marshal(input)

	cmd := exec.Command("go", "run", "main.go")

	cmd.Dir = path
	cmd.Stdin = bytes.NewReader(data)

	out, err := cmd.CombinedOutput()

	fmt.Println("====", path, "====")
	fmt.Println(string(out))

	if err != nil {
		fmt.Println("error:", err)
	}
}

func writeTPSResultToExcel(filename string, baselines []string, tpssAndLatency [][][]float64) error {
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
	err = f.SetCellValue(sheet, "C1", "Latency (s)")
	if err != nil {
		return err
	}

	for i := 0; i < len(baselines) && i < len(tpssAndLatency); i++ {
		row := i + 2
		err = f.SetCellValue(sheet, fmt.Sprintf("A%d", row), baselines[i])
		if err != nil {
			return err
		}
		err = f.SetCellValue(sheet, fmt.Sprintf("B%d", row), tpssAndLatency[i][0][0])
		if err != nil {
			return err
		}
		err = f.SetCellValue(sheet, fmt.Sprintf("C%d", row), tpssAndLatency[i][1][0])
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

func run(baseline, baseFileName string, tpss *[][][]float64, signalChan chan struct{}, signalWg *sync.WaitGroup, blockTxs []types.Transactions, levm *lvm.LEVM) {
	monitorFilePath := filepath.Join(janusConfig.MonitorBasePath, baseline+"/"+baseFileName)
	if baseline == "Non_Prioritied" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, janus_calssic_occ.Run(blockTxs, levm))
	} else if baseline == "Non_Concurrent_Graph_Construct" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, janusClassicDAG.Run(blockTxs, levm))
	} else if baseline == "Non_Maximum_Commit_Validation" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, janusClassicAbort.Run(blockTxs, levm))
	} else if baseline == "MIEX" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, optme.Run(blockTxs, levm))
	}
	//if baseline == "harmony" {
	//	go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
	//	*tpss = append(*tpss, harmony.Run(blockTxs, levm))
	//} else if baseline == "schain" {
	//	go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
	//	*tpss = append(*tpss, schain.Run(blockTxs, levm))
	//} else if baseline == "optme" {
	//	go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
	//	*tpss = append(*tpss, optme.Run(blockTxs, levm))
	//} else if baseline == "aria" {
	//	go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
	//	*tpss = append(*tpss, aria.Run(blockTxs, levm))
	//} else if baseline == "serial" {
	//	go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
	//	*tpss = append(*tpss, serial.Run(blockTxs, levm))
	//} else if baseline == "janus" {
	//	go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
	//	*tpss = append(*tpss, janus.Run(blockTxs, levm))
	//}
}

func main() {

	baseline := flag.String("baseline", "all",
		"baseline:\n"+
			"\tall      run all baseline\n"+
			"\tschian   run schain\n"+
			"\toptme    run optme\n"+
			"\taria     run aria\n"+
			"\tharmony  run harmony\n"+
			"\tserial   run serial\n"+
			"\tblockstm run blockstm\n"+
			"\tjanus    run janus")

	fmt.Println(baseline)

	flag.IntVar(&threadNumber, "t", 8, "threads number")
	flag.IntVar(&blockNumber, "b", 10, "blocks number")
	flag.IntVar(&blockTxNumber, "bt", 2000, "transactions per block")

	flag.Float64Var(&skew, "sk", 0.5, "zipf skew")

	flag.IntVar(&addressNumberRate, "ar", 4,
		"address number rate = blockTxSum * addressNumberRate")

	flag.Float64Var(&longTxCountRate, "lr", 0.5,
		"long transaction rate (long + short = 1)")
	flag.Float64Var(&shortTxCountRate, "sr", 0.5,
		"short transaction rate (long + short = 1)")

	flag.Float64Var(&waterMarkAlpha, "wa", 1.5, "water mark alpha")
	flag.Float64Var(&waterMarkBeta, "wb", 3.5, "water mark beta")

	flag.IntVar(&fibonacciN, "f", 10,
		"fibonacci number (-1 means randomly generated, 0 is not available)")
	//flag.IntVar(&fibonacciN, "f", 10,
	//	"short transaction fibonacci number (-1 means randomly generated, 0 is not available)")
	flag.IntVar(&shortTxFibonacciLoopNumber, "sfln", 20,
		"short transaction fibonacci loop number (0 is not available)")
	flag.IntVar(&longTxFibonacciLoopNumber, "lfln", 40,
		"long transaction fibonacci loop number (0 is not available)")

	flag.BoolVar(&recursiveCalculateFibonacci, "r", false,
		"recursive calculate fibonacci (default false)")
	flag.Parse()
	fmt.Println(
		"baseline:", *baseline,
		"\nthreadNumber:", threadNumber,
		"\nblockNumber:", blockNumber,
		"\nblockTxNumber:", blockTxNumber,
		"\nskew:", skew,
		"\naddressNumberRate:", addressNumberRate,
		"\nlongTxCountRate:", longTxCountRate,
		"\nshortTxCountRate:", shortTxCountRate,
		"\nwaterMarkAlpha:", waterMarkAlpha,
		"\nwaterMarkBeta:", waterMarkBeta,
		"\nfibonacciN:", fibonacciN,
		"\nshortTxFibonacciLoopNumber:", shortTxFibonacciLoopNumber,
		"\nlongTxFibonacciLoopNumber:", longTxFibonacciLoopNumber,
		"\nrecursiveCalculateFibonacci:", recursiveCalculateFibonacci,
	)

	if *baseline != "all" && *baseline != "harmony" && *baseline != "schain" && *baseline != "optme" &&
		*baseline != "aria" && *baseline != "serial" && *baseline != "janus" && *baseline != "blockstm" {
		fmt.Println("baseline is invalid")
		return
	}

	blocksInfo := make([][][]int, 0, blockNumber)
	for i := 0; i < blockNumber; i++ {
		// 生成交易（Zipf 控制冲突率）
		// txsInfo [][]int = [from, to, txType, fibonacciN]
		txsInfo := tools.GenerateBaseTransaction(blockTxNumber*addressNumberRate, int(float64(blockTxNumber)*longTxCountRate),
			int(float64(blockTxNumber)*shortTxCountRate), fibonacciN, shortTxFibonacciLoopNumber, longTxFibonacciLoopNumber, skew)
		fmt.Printf("区块%d: 生成交易数量: %d\n", i, len(txsInfo)) // 生成交易基础信息
		blocksInfo = append(blocksInfo, txsInfo)
	}

	input := InputData{
		Baseline:     *baseline,
		ThreadNumber: threadNumber,
		BlockNumber:  blockNumber,
		BlockTxNum:   blockTxNumber,

		Skew: skew,

		WaterMarkAlpha: waterMarkAlpha,
		WaterMarkBeta:  waterMarkBeta,

		LongTxCountRate:  longTxCountRate,
		ShortTxCountRate: shortTxCountRate,

		FibonacciN:                  fibonacciN,
		FibonacciLoopNum:            longTxFibonacciLoopNumber,
		RecursiveCalculateFibonacci: recursiveCalculateFibonacci,

		Txs: blocksInfo,
	}

	//data, _ := io.ReadAll(os.Stdin)
	//
	//err := json.Unmarshal(data, &input)
	//if err != nil {
	//	panic(err)
	//}

	//if *baseline == "blockstm" {
	//	runProject("/root/Janus_blockstm", input)
	//	return
	//}
	//if *baseline != "all" {
	//	fmt.Println("run Janus")
	//	runProject("/root/Janus", input)
	//	return
	//}
	//runProject("/root/Janus", input)
	//runProject("/root/Janus_blockstm", input)
	janusConfig.AllThreadNum = input.ThreadNumber
	janusConfig.Skew = input.Skew
	janusConfig.AllBlocksTxSum = input.BlockNumber * input.BlockTxNum
	janusConfig.BlockSize = input.BlockTxNum
	janusConfig.WaterMarkAlpha = input.WaterMarkAlpha
	janusConfig.WaterMarkBeta = input.WaterMarkBeta

	if janusConfig.AllThreadNum == 0 {
		vm.InitTxCost(1)
	} else {
		vm.InitTxCost(janusConfig.AllThreadNum)
	}

	runtime.GOMAXPROCS(janusConfig.AllThreadNum + 2)
	fmt.Printf("GOMAXPROCS set to: %d\n", runtime.GOMAXPROCS(0))
	var (
		baseFileName = "t(" + strconv.Itoa(input.ThreadNumber) + ")" +
			"_bt(" + strconv.Itoa(input.BlockNumber) + ")" +
			"_sk(" + fmt.Sprintf("%f", input.Skew) + ")" +
			"_lr(" + fmt.Sprintf("%f", input.LongTxCountRate) + ")" +
			"_sr(" + fmt.Sprintf("%f", input.ShortTxCountRate) + ")" +
			"_wa(" + fmt.Sprintf("%f", input.WaterMarkAlpha) + ")" +
			"_wb(" + fmt.Sprintf("%f", input.WaterMarkBeta) + ")" +
			"_f(" + strconv.Itoa(input.FibonacciN) + ")" +
			"_fln(" + strconv.Itoa(input.FibonacciLoopNum) + ")" +
			"_r(" + strconv.FormatBool(input.RecursiveCalculateFibonacci) + ").xlsx"
		tpssAndLatency [][][]float64 = make([][][]float64, 0)
		//baselines                    = []string{"serial", "janus", "harmony", "schain", "optme", "aria"}
		//baselines = []string{"Non_Prioritied", "Non_Concurrent_Graph_Construct", "Non_Maximum_Commit_Validation", "MIEX"}
		baselines = []string{"Non_Maximum_Commit_Validation"}
	)

	blockNum := janusConfig.AllBlocksTxSum / janusConfig.BlockSize
	blockTxs := make([]types.Transactions, 0) // 每个block的交易集合
	for i := 0; i < blockNum; i++ {
		ethTxs := tools.GenerateTxsFormBriefTx(input.Txs[i], input.RecursiveCalculateFibonacci)
		blockTxs = append(blockTxs, ethTxs)
	}

	fmt.Println("正在预读取状态...")
	levm := lvm.New(stateConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())
	for _, txs := range blockTxs {
		lvm.PreReadState(txs, levm)
	}
	defer levm.AllDB().Close()

	if input.Baseline != "all" {
		baselines = []string{input.Baseline}
	}

	for _, bl := range baselines {
		signalChan := make(chan struct{})
		signalWg := new(sync.WaitGroup)
		signalWg.Add(1)
		run(bl, baseFileName, &tpssAndLatency, signalChan, signalWg, blockTxs, levm)
		signalChan <- struct{}{}
		close(signalChan)
		signalWg.Wait()
		fmt.Println()
	}
	err := writeTPSResultToExcel(filepath.Join(janusConfig.MonitorBasePath, "tps"+"/"+baseFileName), baselines, tpssAndLatency)
	if err != nil {
		fmt.Println(err)
	}
}
