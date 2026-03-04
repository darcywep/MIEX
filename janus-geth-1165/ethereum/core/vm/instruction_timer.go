// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.

package vm

import (
	"encoding/json"
	"fmt"
	"janus-geth-1165/config"
	"os"
)

var (
	LoadTxCost        []bool
	TxCost            []float64
	OpCodeCallNumbers [][]int // workerID -> OpCode -> CallNumbers
)

func init() {
	LoadTxCost = make([]bool, 1)
	OpCodeCallNumbers = make([][]int, 1)
}

func InitTxCost(threads int) {
	LoadTxCost = make([]bool, threads)
	OpCodeCallNumbers = make([][]int, threads)
}

func OpenTxCost(workerID int) {
	LoadTxCost[workerID] = true
	OpCodeCallNumbers[workerID] = make([]int, 256)
}

func CloseTxCost(workerID int) (cost float64) {
	LoadTxCost[workerID] = false
	for opCode, number := range OpCodeCallNumbers[workerID] {
		cost += InstructionTimers[opCode].AverageTime * float64(number)
	}
	return
}

func AddOpCodeCallNumber(workerID int, opCode OpCode) {
	OpCodeCallNumbers[workerID][opCode]++
}

// InstructionTiming 存储单个指令的计时信息
type InstructionTiming struct {
	OpCode      OpCode  `json:"-"`
	OpName      string  `json:"op_name"`
	SampleCount int     `json:"sample_count"`
	AverageTime float64 `json:"average_time_ns"`
}

var InstructionTimers []*InstructionTiming
var InstructionAverageTime = float64(50 / 100)

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
