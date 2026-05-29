// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.

package vm

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"Janus/config"

	"github.com/ethereum/go-ethereum/common"
)

var (
	// LoadTxCost 控制当前交易是否统计 opcode 次数。
	// replay 在 TxStart 打开，在 TxEnd 关闭，用来计算 opcode_estimated_ns；
	// Janus 并发执行路径也沿用这组交易级 opcode 计数。
	LoadTxCost        []bool
	TxCost            []float64
	OpCodeCallNumbers [][]int // workerID -> OpCode -> CallNumbers
	// LoadContractLatency 控制当前交易是否统计合约调用栈。
	// 这组字段只由串行 replay 打开，和 Janus 并发执行模块无关。
	LoadContractLatency    []bool
	ContractLatencyStacks  [][]*ContractLatencyTrace
	ContractLatencyResults [][]*ContractLatencyTrace
	// LoadEVMWallTime 控制是否统计顶层 EVM Run 的真实耗时。
	// replay 用 tx_wall_latency 减去它，所以普通转账不会因为没有进入 EVM 而变成 0。
	// 同时 EVM 内部耗时可以再用 InstructionTimers 的指令估算值替换。
	LoadEVMWallTime []bool
	EVMWallTimeNS   []int64
)

func init() {
	LoadTxCost = make([]bool, 1)
	OpCodeCallNumbers = make([][]int, 1)
	LoadContractLatency = make([]bool, 1)
	ContractLatencyStacks = make([][]*ContractLatencyTrace, 1)
	ContractLatencyResults = make([][]*ContractLatencyTrace, 1)
	LoadEVMWallTime = make([]bool, 1)
	EVMWallTimeNS = make([]int64, 1)
}

// InitTxCost 初始化 Janus 旧执行路径按 workerID 访问的交易 opcode 计数数组。
// Janus 并发模块仍会调用 OpenTxCost/CloseTxCost，所以这里保留原函数名以免影响其他模块。
func InitTxCost(threads int) {
	initLatencyTraceSlots(threads)
}

// InitTxLatencyTrace 初始化 replay latency 采集需要的 worker 槽位。
// 串行 replay 只需要传 1，只使用 worker 0；OpenTxLatencyTrace 不会自动扩容。
func InitTxLatencyTrace(threads int) {
	initLatencyTraceSlots(threads)
}

// initLatencyTraceSlots 统一创建交易级 opcode、合约调用树和 EVM wall time 三组数组。
// InitTxCost 和 InitTxLatencyTrace 共用这段逻辑，保证旧 cost 路径和 replay latency 路径看到一致的 worker 布局。
func initLatencyTraceSlots(threads int) {
	LoadTxCost = make([]bool, threads)
	OpCodeCallNumbers = make([][]int, threads)
	LoadContractLatency = make([]bool, threads)
	ContractLatencyStacks = make([][]*ContractLatencyTrace, threads)
	ContractLatencyResults = make([][]*ContractLatencyTrace, threads)
	LoadEVMWallTime = make([]bool, threads)
	EVMWallTimeNS = make([]int64, threads)
}

// TxLatencyTraceResult 是关闭交易 latency 采集后返回给 replay 模块的汇总结果。
// OpcodeLatencyNS 用 InstructionTimers 从 opcode 次数换算，EVMWallTimeNS 是真实顶层 EVM wall time。
type TxLatencyTraceResult struct {
	OpcodeLatencyNS float64
	EVMWallTimeNS   int64
	ContractTraces  []*ContractLatencyTrace
}

// OpenTxLatencyTrace 统一开启串行 replay 一笔交易需要的 latency 统计。
// 这组 latency 入口和旧的 OpenTxCost/CloseTxCost 分开命名，避免和 Janus 的交易 cost 统计混在一起。
func OpenTxLatencyTrace(workerID int) {
	if !validTxLatencyWorker(workerID) {
		fmt.Println("⚠️ 无效的 workerID，无法开启 TxLatencyTrace: ", workerID)
		return
	}
	// 交易级：重置整笔交易的 opcode 次数，TxEnd 时换算为 opcode_latency_ns。
	LoadTxCost[workerID] = true
	OpCodeCallNumbers[workerID] = make([]int, 256)
	// 合约级：重置本交易的 EVM frame 调用树，每个 frame 内部先统计 opcode 次数。
	LoadContractLatency[workerID] = true
	ContractLatencyStacks[workerID] = make([]*ContractLatencyTrace, 0)
	ContractLatencyResults[workerID] = make([]*ContractLatencyTrace, 0)
	// EVM wall time：重置顶层 EVM Run 的真实耗时，用来从交易 wall time 中剥离 EVM 窗口。
	LoadEVMWallTime[workerID] = true
	EVMWallTimeNS[workerID] = 0
}

// CloseTxLatencyTrace 统一关闭串行 replay 一笔交易的 latency 统计，并返回汇总结果。
// 这里集中完成 opcode 次数到耗时的换算、EVM wall time 读取和合约调用树取出。
func CloseTxLatencyTrace(workerID int) TxLatencyTraceResult {
	if !validTxLatencyWorker(workerID) {
		fmt.Println("⚠️ 无效的 workerID，无法关闭 TxLatencyTrace: ", workerID)
		return TxLatencyTraceResult{}
	}
	result := TxLatencyTraceResult{}
	LoadTxCost[workerID] = false
	result.OpcodeLatencyNS = opCodeCallNumbersCost(OpCodeCallNumbers[workerID])

	LoadEVMWallTime[workerID] = false
	result.EVMWallTimeNS = EVMWallTimeNS[workerID]

	LoadContractLatency[workerID] = false
	result.ContractTraces = ContractLatencyResults[workerID]
	ContractLatencyStacks[workerID] = nil
	ContractLatencyResults[workerID] = nil
	return result
}

// OpenTxCost 开启 Janus 旧执行路径中某个已初始化 worker 的交易级 opcode 计数。
// 调用方必须先调用 InitTxCost 初始化足够的 worker 槽位；如果 workerID 越界，本次统计直接跳过。
func OpenTxCost(workerID int) {
	if !validTxCostWorker(workerID) {
		return
	}
	// 为一笔交易重置 opcode 计数器。解释器每执行一条指令都会通过
	// RecordOpCodeExecution 增加一次计数。
	LoadTxCost[workerID] = true
	OpCodeCallNumbers[workerID] = make([]int, 256)
}

// CloseTxCost 关闭 Janus 旧执行路径中某个 worker 的交易级 opcode 计数，并返回 InstructionTimers 估算耗时。
func CloseTxCost(workerID int) (cost float64) {
	if !validTxCostWorker(workerID) {
		return 0
	}
	LoadTxCost[workerID] = false
	if OpCodeCallNumbers[workerID] == nil {
		return 0
	}
	// 把 opcode 次数转换成估计耗时：每条指令的执行次数乘以 InstructionTimers 中的平均耗时。
	return opCodeCallNumbersCost(OpCodeCallNumbers[workerID])
}

// AddOpCodeCallNumber 是旧调用路径保留的单条 opcode 计数入口。
// 新的 replay 统计统一走 RecordOpCodeExecution，用于同时更新交易和合约 frame。
func AddOpCodeCallNumber(workerID int, opCode OpCode) {
	if !validTxCostWorker(workerID) {
		return
	}
	if OpCodeCallNumbers[workerID] == nil {
		OpCodeCallNumbers[workerID] = make([]int, 256)
	}
	OpCodeCallNumbers[workerID][opCode]++
}

// RecordOpCodeExecution 是解释器执行 opcode 时的统一计时入口。
// 它只在对应 worker 的统计开关已打开时工作，因此普通 VM 执行不会产生 replay 统计数据。
func RecordOpCodeExecution(workerID int, opCode OpCode) {
	if !validTxCostWorker(workerID) {
		return
	}
	// 交易级统计：每执行一条 opcode 计一次数，TxEnd 时据此计算 opcode_estimated_ns。
	if LoadTxCost[workerID] {
		if OpCodeCallNumbers[workerID] == nil {
			OpCodeCallNumbers[workerID] = make([]int, 256)
		}
		OpCodeCallNumbers[workerID][opCode]++
	}
	// 合约级统计：执行时只记录当前 frame 内每种 opcode 的次数。
	// frame 退出时再统一用 InstructionTimers 换算 LatencyNS，避免执行循环里反复做耗时累加。
	if validContractLatencyWorker(workerID) && LoadContractLatency[workerID] && len(ContractLatencyStacks[workerID]) > 0 {
		frame := ContractLatencyStacks[workerID][len(ContractLatencyStacks[workerID])-1]
		if frame.opCodeCallNumbers == nil {
			frame.opCodeCallNumbers = make([]int, 256)
		}
		frame.opCodeCallNumbers[opCode]++
	}
}

// OpCodeAverageTime 返回 replay 估算使用的单条 opcode 平均耗时。
// 如果某条 opcode 没有计时数据，则使用 InstructionAverageTime 兜底。
func OpCodeAverageTime(opCode OpCode) float64 {
	op := int(opCode)
	if op >= 0 && op < len(InstructionTimers) && InstructionTimers[op] != nil {
		return InstructionTimers[op].AverageTime
	}
	return InstructionAverageTime
}

// ContractLatencyTrace 表示一个 EVM 执行 frame。
// LatencyNS 只包含当前 frame 自己执行的 opcode。
// InclusiveLatencyNS 会额外包含所有嵌套子调用的耗时。
type ContractLatencyTrace struct {
	Key                string                  `json:"key"`
	ContractAddress    string                  `json:"contract_address"`
	MethodSelector     string                  `json:"method_selector"`
	LatencyNS          float64                 `json:"latency_ns"`
	InclusiveLatencyNS float64                 `json:"inclusive_latency_ns"`
	Children           []*ContractLatencyTrace `json:"children,omitempty"`
	// opCodeCallNumbers 只在内存中保存当前 frame 的 opcode 次数，退出 frame 时换算为 LatencyNS。
	// 它不写入 JSON，避免 contract_trace 里为每个 frame 存 256 个计数字段。
	opCodeCallNumbers []int
}

// AddEVMWallTime 累加 interpreter.Run 传入的 EVM 执行真实耗时。
// 当前只由顶层 Run 调用，避免嵌套调用被重复计入 evm_wall_latency_ns。
func AddEVMWallTime(workerID int, duration time.Duration) {
	if !validEVMWallTimeWorker(workerID) {
		return
	}
	if !LoadEVMWallTime[workerID] {
		return
	}
	// 只有 interpreter.Run 决定哪些 duration 会被统计。目前只记录 depth==1，
	// 因为顶层 Run 的耗时已经包含所有嵌套调用，避免重复计算。
	EVMWallTimeNS[workerID] += duration.Nanoseconds()
}

// EnterContractLatencyFrame 把一个 EVM 执行 frame 压入当前交易的调用树。
// key 使用 contractAddress_methodSelector，方便 replay 按合约入口聚合平均耗时。
func EnterContractLatencyFrame(workerID int, address common.Address, input []byte, isDeployment bool) {
	if !validContractLatencyWorker(workerID) {
		return
	}
	if !LoadContractLatency[workerID] {
		return
	}
	selector := contractMethodSelector(input, isDeployment)
	// 当前 frame 只先创建 opcode 计数桶；LatencyNS 会在退出 frame 时统一换算。
	// InclusiveLatencyNS 也会在退出 frame 时把子调用耗时加进去。
	frame := &ContractLatencyTrace{
		Key:               address.Hex() + "_" + selector,
		ContractAddress:   address.Hex(),
		MethodSelector:    selector,
		opCodeCallNumbers: make([]int, 256),
	}
	stack := ContractLatencyStacks[workerID]
	if len(stack) == 0 {
		// 没有父 frame，说明这是本交易的一棵根调用树。
		ContractLatencyResults[workerID] = append(ContractLatencyResults[workerID], frame)
	} else {
		// 嵌套调用挂到父 frame 下；后续仍会按 contractAddress_methodSelector 独立聚合。
		parent := stack[len(stack)-1]
		parent.Children = append(parent.Children, frame)
	}
	ContractLatencyStacks[workerID] = append(stack, frame)
}

// ExitContractLatencyFrame 关闭当前 EVM frame，并在所有子调用已经退出后计算 inclusive 耗时。
func ExitContractLatencyFrame(workerID int) {
	if !validContractLatencyWorker(workerID) || !LoadContractLatency[workerID] {
		return
	}
	stack := ContractLatencyStacks[workerID]
	if len(stack) == 0 {
		return
	}
	frame := stack[len(stack)-1]
	// frame 退出时才把 opcode 次数换算成当前 frame 自身耗时。
	// 子 frame 一定先于父 frame 退出，所以这里可以直接累加子 frame 的 InclusiveLatencyNS。
	frame.LatencyNS = opCodeCallNumbersCost(frame.opCodeCallNumbers)
	frame.InclusiveLatencyNS = frame.LatencyNS
	for _, child := range frame.Children {
		frame.InclusiveLatencyNS += child.InclusiveLatencyNS
	}
	ContractLatencyStacks[workerID] = stack[:len(stack)-1]
}

// contractMethodSelector 把 calldata 转成聚合 key 里的 methodSelector 部分。
// 创建合约和 fallback 调用没有普通 ABI selector，因此分别用固定字符串标记。
func contractMethodSelector(input []byte, isDeployment bool) string {
	// 合约创建没有 ABI selector，单独标记为 constructor，避免和 fallback 混在一起。
	if isDeployment {
		return "constructor"
	}
	// calldata 少于 4 字节时不可能包含 ABI selector，按 fallback 处理。
	if len(input) < 4 {
		return "fallback"
	}
	return "0x" + common.Bytes2Hex(input[:4])
}

// opCodeCallNumbersCost 把一组 opcode 次数统一换算成 InstructionTimers 估算耗时。
// 交易级和合约 frame 级统计共用这段逻辑，保证两边的换算公式一致。
func opCodeCallNumbersCost(numbers []int) (cost float64) {
	for opCode, number := range numbers {
		if number == 0 {
			continue
		}
		cost += OpCodeAverageTime(OpCode(opCode)) * float64(number)
	}
	return cost
}

// InstructionTiming 存储单个指令的计时信息
type InstructionTiming struct {
	OpCode      OpCode  `json:"-"`
	OpName      string  `json:"op_name"`
	SampleCount int     `json:"sample_count"`
	AverageTime float64 `json:"average_time_ns"`
}

var InstructionTimers []*InstructionTiming
var InstructionAverageTime = float64(50) / 100

func init() {
	timings, err := LoadTimings(config.InstructionTimingFilePath)
	if err != nil {
		fmt.Printf("⚠️ 无法加载指令计时数据: %v\n", err)
		InstructionTimers = make([]*InstructionTiming, len(opCodeToString))
		return
	}

	InstructionTimers = make([]*InstructionTiming, len(opCodeToString))
	for _, timing := range timings {
		timing.OpCode = StringToOp(timing.OpName)
		timing.AverageTime = timing.AverageTime / 100
		InstructionTimers[timing.OpCode] = timing
		//fmt.Println(timing.OpName, timing.OpCode, timing.SampleCount, timing.AverageTime)
	}
	for opCode, timing := range InstructionTimers {
		if timing == nil {
			newTiming := &InstructionTiming{
				OpCode:      OpCode(opCode),
				OpName:      OpCode(opCode).String(),
				SampleCount: 0,
				AverageTime: InstructionAverageTime,
			}
			InstructionTimers[opCode] = newTiming
		}
	}
}

// validTxCostWorker 判断 workerID 是否落在 InitTxCost 初始化出的交易级统计数组内。
func validTxCostWorker(workerID int) bool {
	return workerID >= 0 && workerID < len(LoadTxCost) && workerID < len(OpCodeCallNumbers)
}

// validTxLatencyWorker 判断 workerID 是否同时拥有交易、合约和 EVM wall time 三类 latency 槽位。
func validTxLatencyWorker(workerID int) bool {
	return validTxCostWorker(workerID) &&
		validContractLatencyWorker(workerID) &&
		validEVMWallTimeWorker(workerID)
}

// validContractLatencyWorker 判断 workerID 是否可以访问 replay 专用的合约调用树统计数组。
func validContractLatencyWorker(workerID int) bool {
	return workerID >= 0 &&
		workerID < len(LoadContractLatency) &&
		workerID < len(ContractLatencyStacks) &&
		workerID < len(ContractLatencyResults)
}

// validEVMWallTimeWorker 判断 workerID 是否可以访问 replay 专用的 EVM wall time 统计数组。
func validEVMWallTimeWorker(workerID int) bool {
	return workerID >= 0 && workerID < len(LoadEVMWallTime) && workerID < len(EVMWallTimeNS)
}

// LoadTimings 从文件加载指令计时数据
func LoadTimings(filename string) (map[string]*InstructionTiming, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("无法打开文件: %w", err)
	}
	defer file.Close()

	var timings []*InstructionTiming
	decoder := json.NewDecoder(file)

	if err := decoder.Decode(&timings); err != nil {
		return nil, fmt.Errorf("解析JSON失败: %w", err)
	}

	// 转换为map方便查询
	result := make(map[string]*InstructionTiming)
	for _, timing := range timings {
		result[timing.OpName] = timing
	}

	fmt.Printf("✓ 从 %s 加载了 %d 条指令数据\n", filename, len(result))
	return result, nil
}

// GetTimingByOpCode 根据OpCode获取计时信息
func GetTimingByOpCode(op OpCode) *InstructionTiming {
	return InstructionTimers[op]
}

// PrintTimingSummary 打印计时数据摘要
func PrintTimingSummary() {
	fmt.Println("PrintTimingSummary")
	if len(InstructionTimers) == 0 {
		fmt.Println("没有计时数据")
		return
	}

	fmt.Println("\n╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║  指令计时数据摘要")
	fmt.Println("╠═══════════════════════════════════════════════════════════╣")
	fmt.Printf("║  总指令数: %d\n", len(InstructionTimers))
	fmt.Println("╠═══════════════════════════════════════════════════════════╣")
	fmt.Println("║  Op Name         | Samples | Avg Time (ns)")
	fmt.Println("╠═══════════════════════════════════════════════════════════╣")

	// 找出最快和最慢的指令
	var fastest, slowest *InstructionTiming
	var totalSamples int
	var totalTime float64

	for _, timing := range InstructionTimers {
		fmt.Println(timing.OpCode, timing.OpName, timing.SampleCount, timing.AverageTime)
		totalSamples += timing.SampleCount
		totalTime += timing.AverageTime * float64(timing.SampleCount)

		if fastest == nil || timing.AverageTime < fastest.AverageTime {
			fastest = timing
		}
		if slowest == nil || timing.AverageTime > slowest.AverageTime {
			slowest = timing
		}
	}

	if fastest != nil {
		fmt.Printf("║  最快: %-15s | %7d | %12.2f\n",
			fastest.OpName, fastest.SampleCount, fastest.AverageTime)
	}
	if slowest != nil {
		fmt.Printf("║  最慢: %-15s | %7d | %12.2f\n",
			slowest.OpName, slowest.SampleCount, slowest.AverageTime)
	}

	if totalSamples > 0 {
		avgOverall := totalTime / float64(totalSamples)
		fmt.Printf("║  整体平均:               | %7d | %12.2f\n",
			totalSamples, avgOverall)
	}

	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()
}
