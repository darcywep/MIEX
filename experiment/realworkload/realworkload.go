package realworkload

import (
	"Janus/baselines/aria/aria"
	"Janus/baselines/harmony/harmony"
	newHarmony "Janus/baselines/harmony/new_harmony"
	"Janus/baselines/optme/optme"
	"Janus/baselines/schain/schain"
	"Janus/baselines/serial"
	janusConfig "Janus/config"
	lvm "Janus/core/evm"
	"Janus/ethereum/config"
	"Janus/ethereum/core/types"
	"Janus/ethereum/core/vm"
	"Janus/ethereum/database"
	"Janus/ethereum/replay/replay_gethcopy"
	"Janus/januscore/janus"
	janusClassicAbort "Janus/januscore/janus_classic_abort"
	"Janus/monitor"
	"Janus/tools"
	"flag"
	"fmt"
	"math/big"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
	"github.com/xuri/excelize/v2"
)

const (
	// realEthereumStartBlockNumber 是本次真实以太坊负载实验的起始区块。
	realEthereumStartBlockNumber uint64 = 21000001
	// realEthereumBlockCount 表示从起始区块开始连续读取 10W 个区块。
	realEthereumBlockCount uint64 = 100000
)

var stateConfig *database.StateDBConfig
var chainConfig *params.ChainConfig

func init() {
	stateConfig = &database.StateDBConfig{
		Path:    janusConfig.SmallbankDatabasePath,
		Cache:   16000,
		Handles: 16000,
	}
	chainConfig = config.TestChainConfig
}

// Run 从 LatencyDB 构建真实以太坊负载，并复用现有 baseline 执行框架做模拟执行。
func Run(args []string) error {
	fs := flag.NewFlagSet("ethereum-real", flag.ExitOnError)
	baseline := fs.String("baseline", "janus", "baseline: all, harmony, schain, optme, aria, janus, Non_Maximum_Commit_Validation, newHarmony")
	threadNumber := fs.Int("t", 8, "threads number")
	latencyThresholdUS := fs.Float64("latency", 50, "long/short threshold in microseconds; tx latency < threshold is short, otherwise long")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !validBaseline(*baseline) {
		return fmt.Errorf("baseline is invalid: %s", *baseline)
	}

	janusConfig.AllThreadNum = *threadNumber
	tools.TraceAbort = false
	if janusConfig.AllThreadNum == 0 {
		vm.InitTxCost(1)
	} else {
		vm.InitTxCost(janusConfig.AllThreadNum)
	}
	runtime.GOMAXPROCS(janusConfig.AllThreadNum + 2)

	fmt.Println("========== Ethereum Real Workload ==========")
	fmt.Printf("baseline: %s\n", *baseline)
	fmt.Printf("threads: %d\n", janusConfig.AllThreadNum)
	fmt.Printf("blocks: [%d, %d)\n", realEthereumStartBlockNumber, realEthereumStartBlockNumber+realEthereumBlockCount)
	fmt.Printf("latency threshold: %.2f us\n", *latencyThresholdUS)
	fmt.Printf("GOMAXPROCS set to: %d\n", runtime.GOMAXPROCS(0))

	blockTxs, totalTxs, maxBlockTxs, err := loadEthereumRealBlockTxs(*latencyThresholdUS)
	if err != nil {
		return err
	}
	janusConfig.AllBlocksTxSum = totalTxs
	janusConfig.BlockSize = maxBlockTxs
	if janusConfig.BlockSize == 0 {
		janusConfig.BlockSize = 1
	}
	fmt.Printf("loaded blocks=%d total_txs=%d max_block_txs=%d\n", len(blockTxs), totalTxs, maxBlockTxs)

	levm := lvm.New(stateConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())
	defer levm.AllDB().Close()

	baselines := []string{"janus", "optme", "newHarmony", "Non_Maximum_Commit_Validation"}
	if *baseline != "all" {
		baselines = []string{*baseline}
	}
	baseFileName := "ethereum_real_start(" + strconv.FormatUint(realEthereumStartBlockNumber, 10) + ")" +
		"_blocks(" + strconv.FormatUint(realEthereumBlockCount, 10) + ")" +
		"_t(" + strconv.Itoa(janusConfig.AllThreadNum) + ")" +
		"_latency_us(" + fmt.Sprintf("%.2f", *latencyThresholdUS) + ").xlsx"

	tpssAndLatency := make([][][]float64, 0, len(baselines))
	for _, bl := range baselines {
		signalChan := make(chan struct{})
		signalWg := new(sync.WaitGroup)
		signalWg.Add(1)
		runBaseline(bl, baseFileName, &tpssAndLatency, signalChan, signalWg, blockTxs, levm)
		signalChan <- struct{}{}
		close(signalChan)
		signalWg.Wait()
		fmt.Println()
	}
	return writeTPSResultToExcel(filepath.Join(janusConfig.MonitorBasePath, "tps"+"/"+baseFileName), baselines, tpssAndLatency)
}

// loadEthereumRealBlockTxs 按区块从 LatencyDB 读取真实交易 latency/rw，并转为模拟交易。
func loadEthereumRealBlockTxs(latencyThresholdUS float64) ([]types.Transactions, int, int, error) {
	reader, err := replay_gethcopy.NewReplayLatencyReader()
	if err != nil {
		return nil, 0, 0, err
	}
	defer reader.Close()
	fmt.Printf("LatencyDB: %s\n", reader.Path())

	blockTxs := make([]types.Transactions, 0, realEthereumBlockCount)
	totalTxs := 0
	maxBlockTxs := 0
	for offset := uint64(0); offset < realEthereumBlockCount; offset++ {
		blockNumber := realEthereumStartBlockNumber + offset
		blockValue, err := replay_gethcopy.ReadReplayBlockLatency(reader, blockNumber)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("读取真实负载区块失败 block=%d: %w", blockNumber, err)
		}
		ethTxs := make(types.Transactions, 0, len(blockValue.Txs))
		for _, record := range blockValue.Txs {
			ethTxs = append(ethTxs, newEthereumSimulatedTx(blockNumber, record.TxIndex, record.LatencyNS, record.ReadAddresses, record.WriteAddresses, latencyThresholdUS))
		}
		blockTxs = append(blockTxs, ethTxs)
		totalTxs += len(ethTxs)
		if len(ethTxs) > maxBlockTxs {
			maxBlockTxs = len(ethTxs)
		}
		if (offset+1)%1000 == 0 {
			fmt.Printf("loaded %d/%d blocks, total_txs=%d\n", offset+1, realEthereumBlockCount, totalTxs)
		}
	}
	return blockTxs, totalTxs, maxBlockTxs, nil
}

// newEthereumSimulatedTx 将 LatencyDB 中的一条 tx 记录转成 baseline 可识别的模拟交易。
func newEthereumSimulatedTx(blockNumber uint64, txIndex int, latencyNS float64, readAddresses, writeAddresses []string, latencyThresholdUS float64) *types.Transaction {
	from := chooseSimulationAddress(readAddresses, writeAddresses, blockNumber, txIndex, 0)
	to := chooseSimulationAddress(writeAddresses, readAddresses, blockNumber, txIndex, 1)
	if from == to {
		to = common.BigToAddress(new(big.Int).SetUint64(blockNumber + uint64(txIndex) + 1))
	}

	gasPrice := big.NewInt(1)
	tx := types.NewTransaction(uint64(txIndex), to, big.NewInt(0), tools.Uint64, gasPrice, nil)
	tx.SetFrom(from)
	tx.SmallBankTo = to
	latencyNSInt := tools.NormalizeSimulationLatencyNS(latencyNS)
	readSet, writeSet := normalizeSimulationRW(readAddresses, writeAddresses, from, to)
	tx.SetSimulation(latencyNSInt, readSet, writeSet)
	if latencyNS/1000 < latencyThresholdUS {
		tx.TxType = janusConfig.ShortTx
	} else {
		tx.TxType = janusConfig.LongTx
	}
	tools.FillTransactionReadWriteKeys(tx)
	return tx
}

// normalizeSimulationRW 保证即使某条记录读写集为空，也至少有 from/to 地址供调度器构图。
func normalizeSimulationRW(readAddresses, writeAddresses []string, from, to common.Address) ([]string, []string) {
	readSet := uniqueAddressStrings(readAddresses)
	writeSet := uniqueAddressStrings(writeAddresses)
	if len(readSet) == 0 {
		readSet = append(readSet, from.Hex())
		if from != to {
			readSet = append(readSet, to.Hex())
		}
	}
	if len(writeSet) == 0 {
		writeSet = append(writeSet, to.Hex())
	}
	return readSet, writeSet
}

func chooseSimulationAddress(primary, secondary []string, blockNumber uint64, txIndex int, salt uint64) common.Address {
	for _, addr := range primary {
		if common.IsHexAddress(addr) {
			return common.HexToAddress(addr)
		}
	}
	for _, addr := range secondary {
		if common.IsHexAddress(addr) {
			return common.HexToAddress(addr)
		}
	}
	return common.BigToAddress(new(big.Int).SetUint64(blockNumber + uint64(txIndex) + salt))
}

func uniqueAddressStrings(addresses []string) []string {
	seen := make(map[string]struct{}, len(addresses))
	result := make([]string, 0, len(addresses))
	for _, address := range addresses {
		if address == "" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		result = append(result, address)
	}
	return result
}

func validBaseline(baseline string) bool {
	switch baseline {
	case "all", "harmony", "schain", "optme", "aria", "serial", "janus", "Non_Maximum_Commit_Validation", "newHarmony":
		return true
	default:
		return false
	}
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
	}
}

func writeTPSResultToExcel(filename string, baselines []string, tpssAndLatency [][][]float64) error {
	f := excelize.NewFile()
	sheet := "TPS"
	_, err := f.NewSheet(sheet)
	if err != nil {
		return err
	}
	if err = f.SetCellValue(sheet, "A1", "Baseline"); err != nil {
		return err
	}
	if err = f.SetCellValue(sheet, "B1", "TPS"); err != nil {
		return err
	}
	if err = f.SetCellValue(sheet, "C1", "Latency (s)"); err != nil {
		return err
	}
	for i := 0; i < len(baselines) && i < len(tpssAndLatency); i++ {
		row := i + 2
		if err = f.SetCellValue(sheet, fmt.Sprintf("A%d", row), baselines[i]); err != nil {
			return err
		}
		if err = f.SetCellValue(sheet, fmt.Sprintf("B%d", row), tpssAndLatency[i][0][0]); err != nil {
			return err
		}
		if err = f.SetCellValue(sheet, fmt.Sprintf("C%d", row), tpssAndLatency[i][1][0]); err != nil {
			return err
		}
	}
	if err = f.DeleteSheet("Sheet1"); err != nil {
		return err
	}
	return f.SaveAs(filename)
}
