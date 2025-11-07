package main

import (
	"aria-go/aria"
	"fmt"
	"time"
)

func main() {
	// 创建示例的事务和区块
	var blocks []*aria.Block
	for b := 0; b < 2; b++ {
		var txs []*aria.Transaction
		for t := 0; t < 6; t++ {
			tr := &aria.Transaction{
				HyperId:   uint64(b*10 + t),
				ReadKeys:  map[string]struct{}{"a": {}},
				WriteKeys: map[string]struct{}{"x": {}},
			}
			txs = append(txs, tr)
		}
		blocks = append(blocks, &aria.Block{Txs: txs})
	}

	stats := &aria.Statistics{}
	ariaInstance := aria.NewAria(blocks, stats, 3, 4, true)
	ariaInstance.Start()
	// 让 worker 稍作时间处理（实际生产环境要用等待）
	time.Sleep(500 * time.Millisecond)
	ariaInstance.Stop()

	fmt.Println("Aria Protocol completed.")
}
