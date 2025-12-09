// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.

package vm

import (
	"encoding/json"
	"fmt"
	"os"
)

const (
	MaxSamplesPerInstruction = 10000
	TimingDataFile           = "instruction_timings.json"
)

// InstructionTiming 存储单个指令的计时信息
type InstructionTiming struct {
	OpCode      OpCode  `json:"-"`
	OpName      string  `json:"op_name"`
	TotalTime   int64   `json:"-"` // 纳秒
	SampleCount int     `json:"sample_count"`
	AverageTime float64 `json:"average_time_ns"`
}

// InstructionTimer 管理所有指令的计时数据
type InstructionTimer struct {
	timings  map[OpCode]*InstructionTiming
	needed   map[OpCode]struct{}
	filename string
}

var (
	globalTimer   *InstructionTimer
	TimingEnabled = false
)

// InitInstructionTimer 初始化全局计时器
func InitInstructionTimer(filename string) {
	globalTimer = &InstructionTimer{
		timings:  make(map[OpCode]*InstructionTiming),
		needed:   make(map[OpCode]struct{}),
		filename: filename,
	}
	TimingEnabled = true
	for opname, opcode := range stringToOp {
		globalTimer.timings[opcode] = &InstructionTiming{
			OpCode: opcode,
			OpName: opname,
		}
		globalTimer.needed[opcode] = struct{}{}
	}
	fmt.Println("=== 指令计时系统已启动 ===")
	fmt.Printf("目标: 每个指令 %d 个样本\n", MaxSamplesPerInstruction)
	fmt.Printf("输出文件: %s\n\n", filename)
}

// RecordGasTiming 记录一次指令计算Gas的时间
func RecordGasTiming(opcode OpCode, duration int64) {
	if !TimingEnabled || globalTimer == nil {
		return
	}

	// 获取或创建计时条目
	timing, exists := globalTimer.timings[opcode]
	if !exists {
		timing = &InstructionTiming{
			OpCode: opcode,
			OpName: opcode.String(),
		}
		globalTimer.timings[opcode] = timing
	}

	// 添加样本
	timing.TotalTime += duration
}

// RecordTiming 记录一次指令执行时间
func RecordTiming(opcode OpCode, duration int64) {
	if !TimingEnabled || globalTimer == nil {
		return
	}

	// 获取或创建计时条目
	timing, exists := globalTimer.timings[opcode]
	if !exists {
		timing = &InstructionTiming{
			OpCode: opcode,
			OpName: opcode.String(),
		}
		globalTimer.timings[opcode] = timing
	}

	// 如果已经收集够样本，跳过
	if timing.SampleCount >= MaxSamplesPerInstruction {
		return
	}

	// 添加样本
	timing.TotalTime += duration
	timing.SampleCount++

	// 达到目标样本数()
	if timing.SampleCount == MaxSamplesPerInstruction {
		timing.AverageTime = float64(timing.TotalTime) / float64(timing.SampleCount)
		fmt.Printf("[完成] %s (0x%02x): %d 个样本, 平均 %.2f ns\n",
			timing.OpName, opcode, timing.SampleCount, timing.AverageTime)
		delete(globalTimer.needed, opcode) // 从需要采样的列表中移除
		// 检查是否所有指令都完成了
		globalTimer.CheckCompletionUnlocked()
	}
}

// CheckCompletionUnlocked 检查是否所有指令都已完成（调用时必须已持有锁）
func (it *InstructionTimer) CheckCompletionUnlocked() {
	allComplete := false
	if len(it.needed) == 0 {
		allComplete = true
	}

	if allComplete {

		fmt.Println("所有已执行的指令均已完成采样！")

		err := it.SaveTimings()
		if err != nil {
			fmt.Printf("保存计时数据时出错: %v\n", err)
		}
		os.Exit(0)
	} else {
		fmt.Printf("尚有 %d 个指令未完成采样。\n", len(it.needed))
		for opcode := range it.needed {
			opcodeInfo := globalTimer.timings[opcode]
			fmt.Println(opcode.String(), opcodeInfo.SampleCount) // 输出未完成采样的指令及其当前样本数
		}
	}
}

// SaveTimings 保存计时数据（调用时必须已持有锁）
func (it *InstructionTimer) SaveTimings() error {
	var output []*InstructionTiming
	for _, timing := range it.timings {
		output = append(output, timing)
	}

	file, err := os.Create(it.filename)
	if err != nil {
		fmt.Printf("创建输出文件失败: %v\n", err)
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		fmt.Printf("编码 JSON 失败: %v\n", err)
		return err
	}

	fmt.Printf("\n计时数据已保存到: %s\n", it.filename)
	fmt.Printf("共记录 %d 个指令\n", len(output))

	return nil
}

// GetProgress 获取当前进度
func (it *InstructionTimer) GetProgress() (completed, total int) {

	total = len(it.timings)
	unfinished := len(it.needed)
	return total - unfinished, total
}

// GetInstructionTimer 获取全局计时器实例
func GetInstructionTimer() *InstructionTimer {
	return globalTimer
}

// IsTimingEnabled 返回计时是否启用
func IsTimingEnabled() bool {
	return TimingEnabled
}
