package main

import (
	"Janus/baselines/aria/aria"
	"Janus/baselines/harmony/harmony"
	newHarmony "Janus/baselines/harmony/new_harmony"
	"Janus/baselines/mvschedo"
	"Janus/baselines/optme/optme"
	optmePaper "Janus/baselines/optme_paper/optme_paper"
	"Janus/baselines/quecc/quecc"
	"Janus/baselines/schain/schain"
	"Janus/baselines/serial"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/core/types"
	"Janus/ethereum/core/vm"
	"Janus/ethereum/database"
	"Janus/januscore/janus"
	janusClassicAbort "Janus/januscore/janus_classic_abort"
	"Janus/monitor"
	"Janus/tools"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xuri/excelize/v2"
)

const (
	baselineMVSchedO = "mvschedo"
	baselineQueCC    = "quecc"
)

var stateConfig *database.StateDBConfig

type InputData struct {
	Baseline     string
	ThreadNumber int
	BlockNumber  int
	BlockTxNum   int

	Skew              float64
	AddressNumberRate int

	WaterMarkAlpha float64
	WaterMarkBeta  float64

	LongTxCountRate  float64
	ShortTxCountRate float64

	FibonacciN                  int
	LongTxFibonacciLoopNumber   int
	ShortTxFibonacciLoopNumber  int
	RecursiveCalculateFibonacci bool
	TraceAbort                  bool

	Txs [][][]int
}

func init() {
	stateConfig = &database.StateDBConfig{
		Path:    janusConfig.SmallbankDatabasePath,
		Cache:   16000,
		Handles: 16000,
	}
}

func main() {
	mode, args := splitMode(os.Args[1:])
	var err error
	switch mode {
	case "synthetic", "old":
		err = runSyntheticFromStdin()
	case "ethereum", "real", "realworkload":
		err = runExistingExperimentEthereum(args)
	case "help", "-h", "--help":
		printUsage()
		return
	default:
		err = fmt.Errorf("unknown workload mode: %s", mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func splitMode(args []string) (string, []string) {
	if len(args) == 0 {
		return "synthetic", args
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		return "help", nil
	}
	if strings.HasPrefix(args[0], "-") {
		return "synthetic", args
	}
	return args[0], args[1:]
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  go run . synthetic < workload.json")
	fmt.Println("  go run . ethereum [ethereum real workload flags]")
}

func runExistingExperimentEthereum(args []string) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	experimentPath := filepath.Join(filepath.Dir(wd), "experiment")
	goArgs := append([]string{"run", ".", "ethereum"}, args...)
	cmd := exec.Command("go", goArgs...)
	cmd.Dir = experimentPath
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	fmt.Println("====", experimentPath, "go", strings.Join(goArgs, " "), "====")
	return cmd.Run()
}

func runSyntheticFromStdin() error {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return err
	}

	var input InputData
	if err := json.Unmarshal(data, &input); err != nil {
		return err
	}
	if !validSyntheticBaseline(input.Baseline) {
		return fmt.Errorf("baseline is invalid: %s", input.Baseline)
	}
	if input.ThreadNumber <= 0 {
		return fmt.Errorf("threads number must be greater than 0")
	}
	if input.BlockNumber <= 0 {
		return fmt.Errorf("blocks number must be greater than 0")
	}
	if len(input.Txs) == 0 {
		return fmt.Errorf("synthetic workload is empty")
	}
	if len(input.Txs) != input.BlockNumber {
		return fmt.Errorf("synthetic workload block count mismatch: blockNumber=%d tx_blocks=%d", input.BlockNumber, len(input.Txs))
	}
	if input.BlockTxNum <= 0 {
		return fmt.Errorf("transactions per block must be greater than 0")
	}

	fmt.Println("========== Janus/MIEX Synthetic Workload Runner ==========")
	fmt.Printf("baseline: %s\n", input.Baseline)
	fmt.Printf("threads: %d\n", input.ThreadNumber)
	fmt.Printf("blocks: %d\n", input.BlockNumber)
	fmt.Printf("transactions per block: %d\n", input.BlockTxNum)

	janusConfig.AllThreadNum = input.ThreadNumber
	janusConfig.Skew = input.Skew
	janusConfig.AllBlocksTxSum = input.BlockNumber * input.BlockTxNum
	janusConfig.BlockSize = input.BlockTxNum
	janusConfig.WaterMarkAlpha = input.WaterMarkAlpha
	janusConfig.WaterMarkBeta = input.WaterMarkBeta
	tools.TraceAbort = input.TraceAbort

	if janusConfig.AllThreadNum == 0 {
		vm.InitTxCost(1)
	} else {
		vm.InitTxCost(janusConfig.AllThreadNum)
	}
	runtime.GOMAXPROCS(janusConfig.AllThreadNum + 2)
	fmt.Printf("GOMAXPROCS set to: %d\n", runtime.GOMAXPROCS(0))

	blockNum := janusConfig.AllBlocksTxSum / janusConfig.BlockSize
	blockTxs := make([]types.Transactions, 0, blockNum)
	for i := 0; i < blockNum; i++ {
		blockTxs = append(blockTxs, tools.GenerateTxsFormBriefTx(input.Txs[i], input.RecursiveCalculateFibonacci))
	}

	fmt.Println("正在预读取状态...")
	levm := lvm.New(stateConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())
	for _, txs := range blockTxs {
		lvm.PreReadState(txs, levm)
	}
	defer levm.AllDB().Close()

	baselines := syntheticBaselines(input.Baseline)
	baseFileName := syntheticWorkloadFileName(input)
	tpssAndLatency := make([][][]float64, 0, len(baselines))

	for i, baseline := range baselines {
		baselineStart := time.Now()
		fmt.Printf("[Baseline %d/%d] start %s\n", i+1, len(baselines), baseline)
		signalChan := make(chan struct{})
		signalWg := new(sync.WaitGroup)
		signalWg.Add(1)
		runBaseline(baseline, baseFileName, &tpssAndLatency, signalChan, signalWg, blockTxs, levm)
		close(signalChan)
		signalWg.Wait()
		fmt.Printf("[Baseline %d/%d] done %s, duration=%v\n", i+1, len(baselines), baseline, time.Since(baselineStart))
		fmt.Println()
	}
	printTPSSummary(baselines, tpssAndLatency)
	return writeTPSResultToExcel(filepath.Join(janusConfig.MonitorBasePath, "tps"+"/"+baseFileName), baselines, tpssAndLatency)
}

func validSyntheticBaseline(baseline string) bool {
	switch baseline {
	case "all", "harmony", "schain", "serial", "optme", "optme_paper", "aria", "janus", "Non_Maximum_Commit_Validation", "newHarmony", baselineMVSchedO, baselineQueCC:
		return true
	default:
		return false
	}
}

func syntheticBaselines(baseline string) []string {
	if baseline != "all" {
		return []string{baseline}
	}
	return []string{"harmony", "schain", "serial", "optme", "optme_paper", "aria", "janus", "Non_Maximum_Commit_Validation", "newHarmony", baselineMVSchedO, baselineQueCC}
}

func syntheticWorkloadFileName(input InputData) string {
	return "synthetic_t(" + strconv.Itoa(input.ThreadNumber) + ")" +
		"_b(" + strconv.Itoa(len(input.Txs)) + ")" +
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
}

func runBaseline(baseline, baseFileName string, tpss *[][][]float64, signalChan chan struct{}, signalWg *sync.WaitGroup, blockTxs []types.Transactions, levm *lvm.LEVM) {
	monitorFilePath := filepath.Join(janusConfig.MonitorBasePath, baseline+"/"+baseFileName)
	if baseline == "harmony" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		*tpss = append(*tpss, harmony.Run(blockTxs, levm))
	} else if baseline == "schain" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		*tpss = append(*tpss, schain.Run(blockTxs, levm))
	} else if baseline == "serial" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		*tpss = append(*tpss, serial.Run(blockTxs, levm))
	} else if baseline == "optme" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		*tpss = append(*tpss, optme.Run(blockTxs, levm))
	} else if baseline == "optme_paper" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		*tpss = append(*tpss, optmePaper.Run(blockTxs, levm))
	} else if baseline == "aria" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		*tpss = append(*tpss, aria.Run(blockTxs, levm))
	} else if baseline == "janus" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		*tpss = append(*tpss, janus.Run(blockTxs, levm))
	} else if baseline == "Non_Maximum_Commit_Validation" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		*tpss = append(*tpss, janusClassicAbort.Run(blockTxs, levm))
	} else if baseline == "newHarmony" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		*tpss = append(*tpss, newHarmony.Run(blockTxs, levm))
	} else if baseline == baselineMVSchedO {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		*tpss = append(*tpss, mvschedo.Run(blockTxs, levm))
	} else if baseline == baselineQueCC {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		*tpss = append(*tpss, quecc.Run(blockTxs, levm))
	}
}

func writeTPSResultToExcel(filename string, baselines []string, tpssAndLatency [][][]float64) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	f := excelize.NewFile()
	sheet := "TPS"
	if _, err := f.NewSheet(sheet); err != nil {
		return err
	}
	if err := f.SetCellValue(sheet, "A1", "Baseline"); err != nil {
		return err
	}
	if err := f.SetCellValue(sheet, "B1", "TPS"); err != nil {
		return err
	}
	if err := f.SetCellValue(sheet, "C1", "Latency (s)"); err != nil {
		return err
	}
	for i, baseline := range baselines {
		row := i + 2
		if err := f.SetCellValue(sheet, fmt.Sprintf("A%d", row), baseline); err != nil {
			return err
		}
		if i >= len(tpssAndLatency) || len(tpssAndLatency[i]) < 2 ||
			len(tpssAndLatency[i][0]) == 0 || len(tpssAndLatency[i][1]) == 0 {
			continue
		}
		if err := f.SetCellValue(sheet, fmt.Sprintf("B%d", row), tpssAndLatency[i][0][0]); err != nil {
			return err
		}
		if err := f.SetCellValue(sheet, fmt.Sprintf("C%d", row), tpssAndLatency[i][1][0]); err != nil {
			return err
		}
	}
	if err := f.DeleteSheet("Sheet1"); err != nil {
		return err
	}
	return f.SaveAs(filename)
}

func printTPSSummary(baselines []string, tpssAndLatency [][][]float64) {
	fmt.Println("========== Baseline TPS Summary ==========")
	fmt.Printf("%-34s %16s %16s\n", "Baseline", "TPS", "Latency(s)")
	for i, baseline := range baselines {
		if i >= len(tpssAndLatency) || len(tpssAndLatency[i]) < 2 ||
			len(tpssAndLatency[i][0]) == 0 || len(tpssAndLatency[i][1]) == 0 {
			fmt.Printf("%-34s %16s %16s\n", baseline, "N/A", "N/A")
			continue
		}
		fmt.Printf("%-34s %16.2f %16.6f\n", baseline, tpssAndLatency[i][0][0], tpssAndLatency[i][1][0])
	}
	fmt.Println("==========================================")
}
