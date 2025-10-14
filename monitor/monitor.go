package monitor

import (
	"fmt"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
)

// MonitorMetrics 监控 CPU 和磁盘利用率
func MonitorMetrics(interval time.Duration) {
	go func() {
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
					fmt.Printf("[%s] CPU: %.2f%% | Disk: %.2f%%\n",
						name, cpuPercent[0], diskUtil)
				}
			}

			prev = now
			time.Sleep(interval)
		}
	}()
}
