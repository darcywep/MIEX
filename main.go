package main

import (
	"Janus/baselines/aria/aria"
	"Janus/baselines/harmony/harmony"
	"Janus/baselines/optme/optme"
	"Janus/baselines/schain/schain"
	"Janus/config"
	"Janus/monitor"
	"flag"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/xuri/excelize/v2"
)

func skewFromBias(bias float64) float64 {
	if bias < 0 {
		bias = 0
	}
	if bias > 1 {
		bias = 1
	}

	const minSkew = 1.0
	const maxSkew = 3.5

	// 指数映射，低 bias 几乎均匀，高 bias 快速倾斜
	return minSkew * math.Pow(maxSkew/minSkew, bias)
}

func run(baseline, baseFileName string, tpss *[]float64, signalChan chan struct{}, signalWg *sync.WaitGroup) {
	monitorFilePath := filepath.Join(config.MonitorBasePath, baseline+"/"+baseFileName)
	if baseline == "harmony" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, harmony.Run())
	} else if baseline == "schain" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, schain.Run())
	} else if baseline == "optme" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, optme.Run())
	} else if baseline == "aria" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg) // 监控 CPU 和磁盘利用率，每秒更新一次
		*tpss = append(*tpss, aria.Run())
	}
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

func main() {
	runtime.GOMAXPROCS(config.AllThreadNum + 2)

	baseline := flag.String("baseline", "janus",
		"mode: (default janus)\n"+
			"\t\"all\" is run all baseline\n"+
			"\t\"schain\" is run schain\n"+
			"\t\"optme\" is run optme\n"+
			"\t\"aria\" is run aria\n"+
			"\t\"harmony\" is run harmony\n")
	threadNumber := flag.String("thread", "8", "thread number(default 8)")
	skew := flag.String("skew", "0.5", "thread number(default 0.5)")
	txNumber := flag.String("txNum", "6000", "thread number(default 6000)")
	flag.Parse()

	fmt.Println("baseline: ", *baseline, "\tthreadNumber: ", *threadNumber,
		"\tskew: ", *skew, "\ttxNumber: ", *txNumber)

	if *baseline != "all" && *baseline != "harmony" && *baseline != "schain" && *baseline != "optme" && *baseline != "aria" {
		fmt.Println("baseline is invalid")
		return
	}

	config.AllThreadNum, _ = strconv.Atoi(*threadNumber)
	config.Skew, _ = strconv.ParseFloat(*skew, 64)
	config.TxNum, _ = strconv.Atoi(*txNumber)

	var (
		baseFileName           = "thread(" + strconv.Itoa(config.AllThreadNum) + ")_skew(" + fmt.Sprintf("%f", config.Skew) + ").xlsx"
		tpss         []float64 = make([]float64, 0)
		baselines              = []string{"harmony", "schain", "optme", "aria"}
	)

	if *baseline != "all" {
		baselines = []string{*baseline}
	}

	for _, bl := range baselines {
		signalChan := make(chan struct{})
		signalWg := new(sync.WaitGroup)
		signalWg.Add(1)
		run(bl, baseFileName, &tpss, signalChan, signalWg)
		close(signalChan)
		signalWg.Wait()
	}
	err := writeTPSResultToExcel(filepath.Join(config.MonitorBasePath, "tps"+"/"+baseFileName), baselines, tpss)
	if err != nil {
		fmt.Println(err)
	}
}
