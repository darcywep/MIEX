// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.

package vm

import (
	"encoding/json"
	"fmt"
	"os"
)

const (
	TimingDataFile = "instruction_timings.json"
)

// InstructionTiming 存储单个指令的计时信息
type InstructionTiming struct {
	OpCode      OpCode  `json:"-"`
	OpName      string  `json:"op_name"`
	SampleCount int     `json:"sample_count"`
	AverageTime float64 `json:"average_time_ns"`
}

var InstructionTimers map[OpCode]*InstructionTiming

func init() {
	timings, err := LoadTimings(TimingDataFile)
	if err != nil {
		fmt.Printf("⚠️ 无法加载指令计时数据: %v\n", err)
		InstructionTimers = make(map[OpCode]*InstructionTiming)
		return
	}

	InstructionTimers = make(map[OpCode]*InstructionTiming)
	for opcode := OpCode(0); opcode <= OpCode(0xff); opcode++ {
		if timing, exists := timings[opcode.String()]; exists {
			timing.OpCode = opcode
			InstructionTimers[opcode] = timing
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
func GetTimingByOpCode(timings map[string]*InstructionTiming, opcode OpCode) *InstructionTiming {
	opname := opcode.String()
	if timing, exists := timings[opname]; exists {
		return timing
	}
	return nil
}

// PrintTimingSummary 打印计时数据摘要
func PrintTimingSummary(timings map[string]*InstructionTiming) {
	if len(timings) == 0 {
		fmt.Println("没有计时数据")
		return
	}

	fmt.Println("\n╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║  指令计时数据摘要")
	fmt.Println("╠═══════════════════════════════════════════════════════════╣")
	fmt.Printf("║  总指令数: %d\n", len(timings))
	fmt.Println("╠═══════════════════════════════════════════════════════════╣")
	fmt.Println("║  Op Name         | Samples | Avg Time (ns)")
	fmt.Println("╠═══════════════════════════════════════════════════════════╣")

	// 找出最快和最慢的指令
	var fastest, slowest *InstructionTiming
	var totalSamples int
	var totalTime float64

	for _, timing := range timings {
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
