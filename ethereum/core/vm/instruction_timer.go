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
	timings    map[OpCode]*InstructionTiming
	needed     map[OpCode]struct{}
	completed  map[OpCode]struct{} // 已完成的指令
	filename   string
	outputFile *os.File // 保持文件打开，用于追加写入
	firstWrite bool     // 是否是第一次写入
}

var (
	globalTimer   *InstructionTimer
	TimingEnabled = false
)

// InitInstructionTimer 初始化全局计时器
func InitInstructionTimer(filename string) {
	globalTimer = &InstructionTimer{
		timings:    make(map[OpCode]*InstructionTiming),
		needed:     make(map[OpCode]struct{}),
		completed:  make(map[OpCode]struct{}),
		filename:   filename,
		firstWrite: true,
	}

	// 初始化所有指令
	TimingEnabled = true
	for opname, opcode := range stringToOp {
		globalTimer.timings[opcode] = &InstructionTiming{
			OpCode: opcode,
			OpName: opname,
		}
		globalTimer.needed[opcode] = struct{}{}
	}

	// 打开输出文件（如果存在则清空）
	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("❌ 无法创建输出文件 %s: %v\n", filename, err)
		return
	}
	globalTimer.outputFile = file

	// 写入JSON数组开始标记
	file.WriteString("[\n")

	fmt.Println("\n╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║        指令计时系统已启动 - 增量写入模式                    ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════╣")
	fmt.Printf("║  目标样本数:    每个指令 %d 个样本\n", MaxSamplesPerInstruction)
	fmt.Printf("║  输出文件:      %s\n", filename)
	fmt.Printf("║  写入模式:      每完成一条指令立即追加写入\n")
	fmt.Printf("║  总指令数:      %d\n", len(globalTimer.timings))
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()
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

	// 如果该指令已完成，跳过
	if _, completed := globalTimer.completed[opcode]; completed {
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

	// 达到目标样本数
	if timing.SampleCount == MaxSamplesPerInstruction {
		timing.AverageTime = float64(timing.TotalTime) / float64(timing.SampleCount)

		// 立即写入这条指令的数据
		if err := globalTimer.appendInstructionToFile(timing); err != nil {
			fmt.Printf("⚠️  写入指令 %s 失败: %v\n", timing.OpName, err)
		} else {
			completed := len(globalTimer.completed) + 1
			total := len(globalTimer.timings)
			percentage := float64(completed) / float64(total) * 100

			fmt.Printf("✓ [%3d/%3d] %.1f%% | %s (0x%02x): %d 样本, 平均 %.2f ns\n",
				completed, total, percentage,
				timing.OpName, opcode, timing.SampleCount, timing.AverageTime)
		}

		// 标记为已完成
		delete(globalTimer.needed, opcode)
		globalTimer.completed[opcode] = struct{}{}

		// 检查是否所有指令都完成了
		globalTimer.checkCompletion()
	}
}

// appendInstructionToFile 追加写入单条指令数据
func (it *InstructionTimer) appendInstructionToFile(timing *InstructionTiming) error {
	if it.outputFile == nil {
		return fmt.Errorf("output file not open")
	}

	// 如果不是第一次写入，添加逗号
	if !it.firstWrite {
		if _, err := it.outputFile.WriteString(",\n"); err != nil {
			return err
		}
	}
	it.firstWrite = false

	// 序列化为JSON
	data, err := json.MarshalIndent(timing, "  ", "  ")
	if err != nil {
		return err
	}

	// 写入文件（带缩进）
	if _, err := it.outputFile.WriteString("  "); err != nil {
		return err
	}
	if _, err := it.outputFile.Write(data); err != nil {
		return err
	}

	// 立即刷新到磁盘
	if err := it.outputFile.Sync(); err != nil {
		return err
	}

	return nil
}

// checkCompletion 检查是否所有指令都已完成
func (it *InstructionTimer) checkCompletion() {
	if len(it.needed) == 0 {
		fmt.Println("\n╔═══════════════════════════════════════════════════════════╗")
		fmt.Println("║           🎉 所有指令采样完成！                            ║")
		fmt.Println("╠═══════════════════════════════════════════════════════════╣")
		fmt.Printf("║  已完成指令数: %d\n", len(it.completed))
		fmt.Printf("║  输出文件:     %s\n", it.filename)
		fmt.Println("╚═══════════════════════════════════════════════════════════╝")

		// 关闭JSON数组
		if it.outputFile != nil {
			it.outputFile.WriteString("\n]\n")
			it.outputFile.Close()
			fmt.Printf("\n✓ 文件已完整保存\n\n")
		}

		os.Exit(0)
	}
}

// CheckCompletionUnlocked 外部调用的检查完成度方法
func (it *InstructionTimer) CheckCompletionUnlocked() {
	if it == nil {
		return
	}

	completed := len(it.completed)
	total := len(it.timings)
	remaining := len(it.needed)
	percentage := float64(completed) / float64(total) * 100

	if remaining == 0 {
		fmt.Println("\n🎉 所有指令采样完成！")

		// 关闭JSON数组
		if it.outputFile != nil {
			it.outputFile.WriteString("\n]\n")
			it.outputFile.Close()
			fmt.Printf("✓ 文件已完整保存到: %s\n\n", it.filename)
		}

		os.Exit(0)
	} else {
		fmt.Println("\n╔═══════════════════════════════════════════════════════════╗")
		fmt.Println("║  采样进度报告")
		fmt.Println("╠═══════════════════════════════════════════════════════════╣")
		fmt.Printf("║  已完成: %3d / %d  (%.1f%%)\n", completed, total, percentage)
		fmt.Printf("║  待完成: %3d\n", remaining)
		fmt.Println("╠═══════════════════════════════════════════════════════════╣")
		fmt.Println("║  未完成的指令 (前10条):")

		count := 0
		for opcode := range it.needed {
			if count >= 10 {
				if remaining > 10 {
					fmt.Printf("║    ... 还有 %d 条未显示\n", remaining-10)
				}
				break
			}
			opcodeInfo := it.timings[opcode]
			fmt.Printf("║    • %-15s (0x%02x): %5d / %d 样本\n",
				opcode.String(), opcode, opcodeInfo.SampleCount, MaxSamplesPerInstruction)
			count++
		}
		fmt.Println("╚═══════════════════════════════════════════════════════════╝")
		fmt.Println()
	}
}

// GetProgress 获取当前进度
func (it *InstructionTimer) GetProgress() (completed, total int) {
	if it == nil {
		return 0, 0
	}

	return len(it.completed), len(it.timings)
}

// GetInstructionTimer 获取全局计时器实例
func GetInstructionTimer() *InstructionTimer {
	return globalTimer
}

// IsTimingEnabled 返回计时是否启用
func IsTimingEnabled() bool {
	return TimingEnabled
}

// Close 关闭计时器并保存剩余数据
func (it *InstructionTimer) Close() error {
	if it == nil {
		return nil
	}

	if it.outputFile != nil {
		// 写入未完成的指令（部分样本）
		hasUnfinished := false
		for opcode, timing := range it.timings {
			if _, completed := it.completed[opcode]; !completed && timing.SampleCount > 0 {
				hasUnfinished = true
				timing.AverageTime = float64(timing.TotalTime) / float64(timing.SampleCount)

				if !it.firstWrite {
					it.outputFile.WriteString(",\n")
				}
				it.firstWrite = false

				data, _ := json.MarshalIndent(timing, "  ", "  ")
				it.outputFile.WriteString("  ")
				it.outputFile.Write(data)

				it.completed[opcode] = struct{}{}
			}
		}

		// 关闭JSON数组
		it.outputFile.WriteString("\n]\n")
		it.outputFile.Close()

		fmt.Println("\n╔═══════════════════════════════════════════════════════════╗")
		fmt.Println("║  计时器关闭")
		fmt.Println("╠═══════════════════════════════════════════════════════════╣")
		fmt.Printf("║  输出文件: %s\n", it.filename)
		fmt.Printf("║  已完成:   %d / %d 指令\n", len(it.completed), len(it.timings))
		if hasUnfinished {
			fmt.Println("║  注意:     包含部分未完成指令的数据")
		}
		fmt.Println("╚═══════════════════════════════════════════════════════════╝")
		fmt.Println()
	}

	return nil
}

// Finalize 在程序退出前调用，确保保存所有数据
func Finalize() {
	if globalTimer != nil {
		globalTimer.Close()
	}
}
