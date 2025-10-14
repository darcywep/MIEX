package monitor

import (
	"fmt"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/xuri/excelize/v2"
)

// MonitorMetrics 监控 CPU 和磁盘利用率
func MonitorMetrics(interval time.Duration, monitor_filename string) {
	go func() {

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

					fmt.Printf("[%s] CPU: %.2f%% | Disk: %.2f%%\n",
						name, cpuPercent[0], diskUtil)

					// 写入 Excel
					f.SetCellValue(sheet, fmt.Sprintf("A%d", row), t)
					f.SetCellValue(sheet, fmt.Sprintf("B%d", row), cpuPercent[0])
					f.SetCellValue(sheet, fmt.Sprintf("E%d", row), diskUtil)
					row++

					if row%10 == 0 { // 每10次写完保存一次
						if err := f.SaveAs(monitor_filename); err != nil {
							fmt.Println("保存 Excel 出错:", err)
						}
					}
				}
			}
			prev = now
			time.Sleep(interval)
		}
	}()
}
