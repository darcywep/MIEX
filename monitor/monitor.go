package monitor

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/xuri/excelize/v2"
)

// MonitorMetrics 监控 CPU 和磁盘利用率
func MonitorMetrics(interval time.Duration, monitorFilePath string, signalChan chan struct{}, signalWg *sync.WaitGroup) {
	defer signalWg.Done()
	runtime.LockOSThread()
	//monitorFilename = monitorFilename + "_CT" + strconv.Itoa(config.ComputingThreadNum) + "_IT" + strconv.Itoa(config.IoThreadNum)
	//monitorFilename = monitorFilename + ".xlsx"
	os.Remove(monitorFilePath)
	// 1. 提取目录并创建（如果不存在）
	dir := filepath.Dir(monitorFilePath)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Printf("创建目录失败: %v\n", err)
			return
		}
	}

	// 创建 Excel 文件
	f := excelize.NewFile()
	sheet := "Sheet1"
	f.SetSheetName(f.GetSheetName(0), sheet)

	// 写入表头
	f.SetCellValue(sheet, "A1", "Time")
	f.SetCellValue(sheet, "B1", "CPU (%)")
	f.SetCellValue(sheet, "C1", "Disk sdb1 (%)")

	row := 2 // 从第二行开始写入数据

	prev, _ := disk.IOCounters()
	time.Sleep(interval)

	for {
		select {
		case <-signalChan:
			if err := f.SaveAs(monitorFilePath); err != nil {
				fmt.Println("保存 Excel 出错:", err)
			}
			return
		default:
			// CPU 利用率
			cpuPercent, _ := cpu.Percent(0, false)

			// 磁盘利用率（基于 IoTime 计算）
			now, _ := disk.IOCounters()
			for name, stat := range now {
				prevStat := prev[name]
				deltaIoTime := float64(stat.IoTime - prevStat.IoTime) // 单位：ms
				// 利用率 = IoTime变化 / 时间间隔（ms）
				diskUtil := deltaIoTime / (float64(interval.Milliseconds())) * 100.0

				if name == "sdb1" {

					t := time.Now().Format("15:04:05")

					//fmt.Printf("[%s] CPU: %.2f%% | Disk: %.2f%%\n",
					//	name, cpuPercent[0], diskUtil)

					// 写入 Excel
					f.SetCellValue(sheet, fmt.Sprintf("A%d", row), t)
					f.SetCellValue(sheet, fmt.Sprintf("B%d", row), cpuPercent[0])
					f.SetCellValue(sheet, fmt.Sprintf("C%d", row), diskUtil)
					row++
				}
			}
			prev = now
			time.Sleep(interval)
		}
	}
}
