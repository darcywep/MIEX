package main

import (
	"Janus/baselines/aria/aria"
	"Janus/baselines/harmony/harmony"
	"Janus/baselines/optme/optme"
	"Janus/baselines/schain/schain"
	"Janus/baselines/serial"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/config"
	"Janus/ethereum/core/types"
	"Janus/ethereum/core/vm"
	"Janus/ethereum/database"
	"Janus/januscore/janus"
	janusClassicDAG "Janus/januscore/janus_classic_dag"
	janus_calssic_occ "Janus/januscore/janus_classic_occ"
	"Janus/monitor"
	"Janus/tools"
	"encoding/json"
	"fmt"
	"io"
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

var stateConfig *database.StateDBConfig
var chainConfig *params.ChainConfig

type InputData struct {
	Baseline     string
	ThreadNumber int
	BlockNumber  int
	BlockTxNum   int

	Skew float64

	AddressNumberRate int

	WaterMarkAlpha float64
	WaterMarkBeta  float64

	LongTxCountRate  float64
	ShortTxCountRate float64

	FibonacciN                  int
	LongTxFibonacciLoopNumber   int
	ShortTxFibonacciLoopNumber  int
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
	if baseline == "harmony" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, harmony.Run(blockTxs, levm))
	} else if baseline == "schain" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, schain.Run(blockTxs, levm))
	} else if baseline == "optme" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, optme.Run(blockTxs, levm))
	} else if baseline == "aria" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, aria.Run(blockTxs, levm))
	} else if baseline == "serial" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, serial.Run(blockTxs, levm))
	} else if baseline == "janus" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, janus.Run(blockTxs, levm))
	} else if baseline == "occ" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, janus_calssic_occ.Run(blockTxs, levm))
	} else if baseline == "serial_construct_graph" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, janusClassicDAG.Run(blockTxs, levm))
	}

}

func main() {
	data, _ := io.ReadAll(os.Stdin)

	var input InputData
	err := json.Unmarshal(data, &input)
	if err != nil {
		panic(err)
	}

	fmt.Println("Janus running, baseline is", input.Baseline)

	if input.Baseline != "all" && input.Baseline != "harmony" && input.Baseline != "schain" && input.Baseline != "optme" &&
		input.Baseline != "aria" && input.Baseline != "serial" && input.Baseline != "janus" {
		fmt.Println("baseline is invalid")
		return
	}

	//fmt.Println(
	//	"baseline:", input.Baseline,
	//	"\nthreadNumber:", input.ThreadNumber,
	//	"\nblockNumber:", input.BlockNumber,
	//	"\nblockTxNumber:", input.BlockTxNum,
	//	"\nskew:", input.Skew,
	//	"\nwaterMarkAlpha:", input.WaterMarkAlpha,
	//	"\nwaterMarkBeta:", input.WaterMarkBeta,
	//	"\nfibonacciN:", input.FibonacciN,
	//	"\nFibonacciLoopNum:", input.FibonacciLoopNum,
	//	"\nrecursiveCalculateFibonacci:", input.RecursiveCalculateFibonacci,
	//	"\ntxs:", input.Txs,
	//)

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
			"_b(" + strconv.Itoa(input.BlockNumber) + ")" +
			"_bt(" + strconv.Itoa(input.BlockTxNum) + ")" +
			"_sk(" + fmt.Sprintf("%.2f", input.Skew) + ")" +
			"_ar(" + strconv.Itoa(input.AddressNumberRate) + ")" +
			"_lr(" + fmt.Sprintf("%.2f", input.LongTxCountRate) + ")" +
			"_sr(" + fmt.Sprintf("%.2f", input.ShortTxCountRate) + ")" +
			"_wa(" + fmt.Sprintf("%.2f", input.WaterMarkAlpha) + ")" +
			"_wb(" + fmt.Sprintf("%.2f", input.WaterMarkBeta) + ")" +
			"_f(" + strconv.Itoa(input.FibonacciN) + ")" +
			"_lfln(" + strconv.Itoa(input.LongTxFibonacciLoopNumber) + ")" +
			"_sfln(" + strconv.Itoa(input.ShortTxFibonacciLoopNumber) + ")" +
			"_r(" + strconv.FormatBool(input.RecursiveCalculateFibonacci) + ").xlsx"
		tpssAndLatency [][][]float64 = make([][][]float64, 0) // baseline -> [tps], [latency], [other(if have)]
		//baselines                    = []string{"serial", "harmony", "schain", "optme", "aria", "occ", "janus", "serial_construct_graph"}
		baselines = []string{"janus", "serial_construct_graph"}
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
		fmt.Println("Baseline is...", bl)
		signalChan := make(chan struct{})
		signalWg := new(sync.WaitGroup)
		signalWg.Add(1)
		run(bl, baseFileName, &tpssAndLatency, signalChan, signalWg, blockTxs, levm)
		signalChan <- struct{}{}
		close(signalChan)
		signalWg.Wait()
		fmt.Println()
	}
	err = writeTPSResultToExcel(filepath.Join(janusConfig.MonitorBasePath, "tps"+"/"+baseFileName), baselines, tpssAndLatency)
	if err != nil {
		fmt.Println(err)
	}
}
