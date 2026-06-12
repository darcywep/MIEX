package realworkload

import (
	"Janus/baselines/aria/aria"
	newHarmony "Janus/baselines/harmony/new_harmony"
	"Janus/baselines/mvschedo"
	"Janus/baselines/optme/optme"
	optmePaper "Janus/baselines/optme_paper/optme_paper"
	"Janus/baselines/pilotfish/pilotfish"
	"Janus/baselines/quecc/quecc"
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
	"os"
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
	// defaultRealEthereumBlockCount 是真实以太坊负载默认连续读取的区块数量。
	defaultRealEthereumBlockCount uint64 = 10000
	baselineMVSchedO                     = "mvschedo"
	baselineQueCC                        = "quecc"
	baselinePilotfish                    = "pilotfish"
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
	baseline := fs.String("baseline", "janus", "baseline: all, harmony(new_harmony), schain, serial, optme, optme_paper, aria, janus, Non_Maximum_Commit_Validation, newHarmony(alias), mvschedo, quecc, pilotfish")
	threadNumber := fs.Int("t", 8, "threads number")
	blockCount := fs.Uint64("b", defaultRealEthereumBlockCount, "number of original ethereum blocks; when -bt > 0, number of regrouped experiment blocks")
	blockTxNumber := fs.Int("bt", 0, "transactions per regrouped block; 0 keeps original ethereum block layout")
	sourceBlockLimit := fs.Uint64("source-b", defaultRealEthereumBlockCount, "max ethereum source blocks to scan when -bt > 0")
	latencyThresholdUS := fs.Float64("latency", 50, "long/short threshold in microseconds; tx latency < threshold is short, otherwise long")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !validBaseline(*baseline) {
		return fmt.Errorf("baseline is invalid: %s", *baseline)
	}
	if *blockCount == 0 {
		return fmt.Errorf("block count must be greater than 0")
	}
	if *blockTxNumber < 0 {
		return fmt.Errorf("transactions per regrouped block must be greater than or equal to 0")
	}
	if *blockTxNumber > 0 && *sourceBlockLimit == 0 {
		return fmt.Errorf("source ethereum block count must be greater than 0 when regrouping blocks")
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
	if *blockTxNumber > 0 {
		fmt.Println("layout: regrouped ethereum txs")
		fmt.Printf("experiment blocks: %d\n", *blockCount)
		fmt.Printf("transactions per experiment block: %d\n", *blockTxNumber)
		fmt.Printf("source ethereum blocks scan limit: [%d, %d)\n", realEthereumStartBlockNumber, realEthereumStartBlockNumber+*sourceBlockLimit)
	} else {
		fmt.Println("layout: original ethereum blocks")
		fmt.Printf("source ethereum blocks: [%d, %d)\n", realEthereumStartBlockNumber, realEthereumStartBlockNumber+*blockCount)
	}
	fmt.Printf("latency threshold: %.2f us\n", *latencyThresholdUS)
	fmt.Printf("GOMAXPROCS set to: %d\n", runtime.GOMAXPROCS(0))

	workload, err := loadEthereumRealWorkload(*latencyThresholdUS, *blockCount, *blockTxNumber, *sourceBlockLimit)
	if err != nil {
		return err
	}
	janusConfig.AllBlocksTxSum = workload.totalTxs
	janusConfig.BlockSize = workload.maxBlockTxs
	if janusConfig.BlockSize == 0 {
		janusConfig.BlockSize = 1
	}
	fmt.Printf("source_blocks_read=%d source_txs_read=%d\n", workload.sourceBlocksRead, workload.sourceTxsRead)
	fmt.Printf("loaded blocks=%d total_txs=%d max_block_txs=%d\n", len(workload.blockTxs), workload.totalTxs, workload.maxBlockTxs)

	levm := lvm.New(stateConfig, big.NewInt(0), tools.StateRoot, tools.GenerateAddress())
	defer levm.AllDB().Close()

	baselines := []string{"harmony", "schain", "serial", "optme", "optme_paper", "aria", "janus", "Non_Maximum_Commit_Validation", baselineMVSchedO, baselineQueCC, baselinePilotfish}
	if *baseline != "all" {
		baselines = []string{*baseline}
	}
	baseFileName := ethereumRealWorkloadFileName(workload, *latencyThresholdUS, janusConfig.AllThreadNum)

	tpssAndLatency := make([][][]float64, 0, len(baselines))
	for i, bl := range baselines {
		baselineStart := time.Now()
		fmt.Printf("[Baseline %d/%d] start %s\n", i+1, len(baselines), bl)
		signalChan := make(chan struct{})
		signalWg := new(sync.WaitGroup)
		signalWg.Add(1)
		runBaseline(bl, baseFileName, &tpssAndLatency, signalChan, signalWg, workload.blockTxs, levm)
		close(signalChan)
		signalWg.Wait()
		fmt.Printf("[Baseline %d/%d] done %s, duration=%v\n", i+1, len(baselines), bl, time.Since(baselineStart))
		fmt.Println()
	}
	printTPSSummary(baselines, tpssAndLatency)
	return writeTPSResultToExcel(filepath.Join(janusConfig.MonitorBasePath, "tps"+"/"+baseFileName), baselines, tpssAndLatency)
}

type ethereumRealWorkload struct {
	blockTxs         []types.Transactions
	totalTxs         int
	maxBlockTxs      int
	sourceBlocksRead uint64
	sourceTxsRead    int
	regrouped        bool
	experimentBlocks uint64
	blockTxNumber    int
}

func ethereumRealWorkloadFileName(workload *ethereumRealWorkload, latencyThresholdUS float64, threadNumber int) string {
	if workload.regrouped {
		return "ethereum_real_regroup_start(" + strconv.FormatUint(realEthereumStartBlockNumber, 10) + ")" +
			"_source_blocks(" + strconv.FormatUint(workload.sourceBlocksRead, 10) + ")" +
			"_b(" + strconv.FormatUint(workload.experimentBlocks, 10) + ")" +
			"_bt(" + strconv.Itoa(workload.blockTxNumber) + ")" +
			"_t(" + strconv.Itoa(threadNumber) + ")" +
			"_latency_us(" + fmt.Sprintf("%.2f", latencyThresholdUS) + ").xlsx"
	}
	return "ethereum_real_start(" + strconv.FormatUint(realEthereumStartBlockNumber, 10) + ")" +
		"_blocks(" + strconv.FormatUint(workload.sourceBlocksRead, 10) + ")" +
		"_t(" + strconv.Itoa(threadNumber) + ")" +
		"_latency_us(" + fmt.Sprintf("%.2f", latencyThresholdUS) + ").xlsx"
}

func loadEthereumRealWorkload(latencyThresholdUS float64, blockCount uint64, blockTxNumber int, sourceBlockLimit uint64) (*ethereumRealWorkload, error) {
	if blockTxNumber > 0 {
		return loadRegroupedEthereumRealBlockTxs(latencyThresholdUS, blockCount, blockTxNumber, sourceBlockLimit)
	}
	return loadOriginalEthereumRealBlockTxs(latencyThresholdUS, blockCount)
}

// loadOriginalEthereumRealBlockTxs 按原始以太坊区块从 LatencyDB 读取真实交易 latency/rw，并转为模拟交易。
// blockCount 由启动参数 -b 控制，表示从 realEthereumStartBlockNumber 开始连续执行多少个原始以太坊区块。
func loadOriginalEthereumRealBlockTxs(latencyThresholdUS float64, blockCount uint64) (*ethereumRealWorkload, error) {
	reader, err := replay_gethcopy.NewReplayLatencyReader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	fmt.Printf("LatencyDB: %s\n", reader.Path())

	blockTxs := make([]types.Transactions, 0, blockCount)
	totalTxs := 0
	maxBlockTxs := 0
	for offset := uint64(0); offset < blockCount; offset++ {
		blockNumber := realEthereumStartBlockNumber + offset
		blockValue, err := replay_gethcopy.ReadReplayBlockLatency(reader, blockNumber)
		if err != nil {
			return nil, fmt.Errorf("读取真实负载区块失败 block=%d: %w", blockNumber, err)
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
			fmt.Printf("loaded %d/%d blocks, total_txs=%d\n", offset+1, blockCount, totalTxs)
		}
	}
	return &ethereumRealWorkload{
		blockTxs:         blockTxs,
		totalTxs:         totalTxs,
		maxBlockTxs:      maxBlockTxs,
		sourceBlocksRead: blockCount,
		sourceTxsRead:    totalTxs,
		regrouped:        false,
		experimentBlocks: blockCount,
		blockTxNumber:    maxBlockTxs,
	}, nil
}

// loadRegroupedEthereumRealBlockTxs 顺序读取真实以太坊交易，并按实验参数重组为固定大小区块。
// blockCount 表示重组后的实验区块数量，blockTxNumber 表示每个实验区块交易数。
func loadRegroupedEthereumRealBlockTxs(latencyThresholdUS float64, blockCount uint64, blockTxNumber int, sourceBlockLimit uint64) (*ethereumRealWorkload, error) {
	maxInt := int(^uint(0) >> 1)
	if blockCount > uint64(maxInt/blockTxNumber) {
		return nil, fmt.Errorf("target transaction count is too large: blocks=%d block_txs=%d", blockCount, blockTxNumber)
	}
	targetTxs := int(blockCount) * blockTxNumber

	reader, err := replay_gethcopy.NewReplayLatencyReader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	fmt.Printf("LatencyDB: %s\n", reader.Path())

	flatTxs := make(types.Transactions, 0, targetTxs)
	sourceTxsRead := 0
	sourceBlocksRead := uint64(0)
	for offset := uint64(0); offset < sourceBlockLimit && len(flatTxs) < targetTxs; offset++ {
		blockNumber := realEthereumStartBlockNumber + offset
		blockValue, err := replay_gethcopy.ReadReplayBlockLatency(reader, blockNumber)
		if err != nil {
			return nil, fmt.Errorf("读取真实负载区块失败 block=%d: %w", blockNumber, err)
		}
		sourceBlocksRead = offset + 1
		sourceTxsRead += len(blockValue.Txs)
		for _, record := range blockValue.Txs {
			if len(flatTxs) >= targetTxs {
				break
			}
			flatTxs = append(flatTxs, newEthereumSimulatedTx(blockNumber, record.TxIndex, record.LatencyNS, record.ReadAddresses, record.WriteAddresses, latencyThresholdUS))
		}
		if sourceBlocksRead%1000 == 0 || len(flatTxs) >= targetTxs {
			fmt.Printf("loaded source_blocks=%d/%d, selected_txs=%d/%d, source_txs_read=%d\n",
				sourceBlocksRead, sourceBlockLimit, len(flatTxs), targetTxs, sourceTxsRead)
		}
	}
	if len(flatTxs) < targetTxs {
		return nil, fmt.Errorf("真实以太坊负载交易数量不足: selected_txs=%d target_txs=%d source_blocks_scanned=%d", len(flatTxs), targetTxs, sourceBlocksRead)
	}

	blockTxs := make([]types.Transactions, 0, blockCount)
	for blockID := uint64(0); blockID < blockCount; blockID++ {
		start := int(blockID) * blockTxNumber
		end := start + blockTxNumber
		blockTxs = append(blockTxs, append(types.Transactions(nil), flatTxs[start:end]...))
	}
	return &ethereumRealWorkload{
		blockTxs:         blockTxs,
		totalTxs:         targetTxs,
		maxBlockTxs:      blockTxNumber,
		sourceBlocksRead: sourceBlocksRead,
		sourceTxsRead:    sourceTxsRead,
		regrouped:        true,
		experimentBlocks: blockCount,
		blockTxNumber:    blockTxNumber,
	}, nil
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
	case "all", "harmony", "schain", "optme", "optme_paper", "aria", "serial", "janus", "Non_Maximum_Commit_Validation", "newHarmony", baselineMVSchedO, baselineQueCC, baselinePilotfish:
		return true
	default:
		return false
	}
}

func runBaseline(baseline, baseFileName string, tpss *[][][]float64, signalChan chan struct{}, signalWg *sync.WaitGroup, blockTxs []types.Transactions, levm *lvm.LEVM) {
	monitorFilePath := filepath.Join(janusConfig.MonitorBasePath, baseline+"/"+baseFileName)
	if baseline == "harmony" {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		// 真实以太坊负载默认把 harmony 映射到 new_harmony，避免旧 Harmony 在大规模真实负载下的等待/回退路径卡住。
		*tpss = append(*tpss, newHarmony.Run(blockTxs, levm))
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
	} else if baseline == baselinePilotfish {
		go monitor.MonitorMetrics(1*time.Second, monitorFilePath, signalChan, signalWg)
		*tpss = append(*tpss, pilotfish.Run(blockTxs, levm))
	}
}

func writeTPSResultToExcel(filename string, baselines []string, tpssAndLatency [][][]float64) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
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
