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
	"Janus/ethereum/database"
	"Janus/januscore/janus"
	"Janus/monitor"
	"Janus/tools"
	"flag"
	"fmt"
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

func init() {
	stateConfig = &database.StateDBConfig{
		Path:    "/root/alldb/smallbank_database",
		Cache:   16000,
		Handles: 16000,
	}
	chainConfig = config.TestChainConfig
}

func writeTPSResultToExcel(filename string, baselines []string, tpss []float64) error {
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

func run(baseline, baseFileName string, tpss *[]float64, signalChan chan struct{}, signalWg *sync.WaitGroup, blockTxs []types.Transactions, levm *lvm.LEVM) {
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
	}
}

func main() {
	baseline := flag.String("baseline", "all",
		"baseline: (default all)\n"+
			"\t\"all\" is run all baseline\n"+
			"\t\"schain\" is run schain\n"+
			"\t\"optme\" is run optme\n"+
			"\t\"aria\" is run aria\n"+
			"\t\"harmony\" is run harmony\n"+
			"\t\"serial\" is run serial\n"+
			"\t\"janus\" is run janus\n")
	threadNumber := flag.String("thread", "8", "thread number(default 8)")
	skew := flag.String("skew", "0.5", "thread number(default 0.5)")
	txNumber := flag.String("txNum", "100000", "thread number(default 6000)")
	flag.Parse()

	fmt.Println("baseline: ", *baseline, "\tthreadNumber: ", *threadNumber,
		"\tskew: ", *skew, "\ttxNumber: ", *txNumber)

	if *baseline != "all" && *baseline != "harmony" && *baseline != "schain" && *baseline != "optme" &&
		*baseline != "aria" && *baseline != "serial" && *baseline != "janus" {
		fmt.Println("baseline is invalid")
		return
	}

	janusConfig.AllThreadNum, _ = strconv.Atoi(*threadNumber)
	janusConfig.Skew, _ = strconv.ParseFloat(*skew, 64)
	janusConfig.TxNum, _ = strconv.Atoi(*txNumber)

	if janusConfig.AllThreadNum == 0 {
		tools.InitTxCost(1)
	} else {
		tools.InitTxCost(janusConfig.AllThreadNum)
	}

	runtime.GOMAXPROCS(janusConfig.AllThreadNum + 2)
	fmt.Printf("GOMAXPROCS set to: %d\n", runtime.GOMAXPROCS(0))
	var (
		baseFileName           = "thread(" + strconv.Itoa(janusConfig.AllThreadNum) + ")_skew(" + fmt.Sprintf("%f", janusConfig.Skew) + ").xlsx"
		tpss         []float64 = make([]float64, 0)
		baselines              = []string{"serial", "harmony", "schain", "optme", "aria", "janus"}
	)

	blockNum := janusConfig.TxNum / janusConfig.BlockSize
	blockTxs := make([]types.Transactions, 0) // 每个block的交易集合
	for i := 0; i < blockNum; i++ {
		txsLen := janusConfig.BlockSize
		// Step 1: 生成地址
		addresses := tools.GenerateAddresses(1, int(float64(txsLen)*janusConfig.AddressNumberRate))
		fmt.Printf("生成地址数量: %d\n", len(addresses))

		// Step 2: 生成交易（Zipf 控制冲突率）
		ethTxs := tools.GenerateSmallBankTxs(addresses, int(float64(txsLen)*janusConfig.CompetingTxCountRate), int(float64(txsLen)*janusConfig.IoTxCountRate),
			janusConfig.FibonacciN, janusConfig.RecursiveCalculateFibonacci, janusConfig.Skew)
		fmt.Printf("生成交易数量: %d\n", len(ethTxs)) // 生成以太坊交易
		blockTxs = append(blockTxs, ethTxs)
	}

	fmt.Println("正在预读取状态...")
	levm := lvm.New(stateConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())
	for _, txs := range blockTxs {
		lvm.PreReadState(txs, levm)
	}
	defer levm.AllDB().Close()

	//config.Skew = skewFromBias(config.Skew)
	if *baseline != "all" {
		baselines = []string{*baseline}
	}

	for _, bl := range baselines {
		signalChan := make(chan struct{})
		signalWg := new(sync.WaitGroup)
		signalWg.Add(1)
		run(bl, baseFileName, &tpss, signalChan, signalWg, blockTxs, levm)
		signalChan <- struct{}{}
		close(signalChan)
		signalWg.Wait()
		fmt.Println()
	}
	err := writeTPSResultToExcel(filepath.Join(janusConfig.MonitorBasePath, "tps"+"/"+baseFileName), baselines, tpss)
	if err != nil {
		fmt.Println(err)
	}
}
