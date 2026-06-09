package main

import (
	"Janus/experiment/realworkload"
	"Janus/experiment/synthetic"
	"fmt"
	"os"
	"strings"
)

// main 是 experiment 的统一启动入口。
// 为了不破坏原来的合成实验启动方式，默认和直接传 flag 时仍运行 synthetic。
// 真实以太坊负载必须显式使用 ethereum/real/realworkload 子命令。
func main() {
	mode, args := splitExperimentMode(os.Args[1:])
	var err error
	switch mode {
	case "ethereum", "real", "realworkload":
		err = realworkload.Run(args)
	case "synthetic", "old":
		err = synthetic.Run(args)
	case "help", "-h", "--help":
		printUsage()
		return
	default:
		err = fmt.Errorf("unknown experiment mode: %s", mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func splitExperimentMode(args []string) (string, []string) {
	if len(args) == 0 {
		return "synthetic", args
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		return "help", nil
	}
	if strings.HasPrefix(args[0], "-") {
		return "synthetic", args
	}
	return args[0], args[1:]
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  go run ./experiment [synthetic flags]")
	fmt.Println("  go run ./experiment synthetic [synthetic flags]")
	fmt.Println("  go run ./experiment ethereum [ethereum real workload flags]")
	fmt.Println()
	fmt.Println("Modes:")
	fmt.Println("  synthetic   原来的合成负载实验；默认模式。直接传 -baseline/-t/-b/-bt 等旧参数仍会进入这里。")
	fmt.Println("  ethereum    真实以太坊负载实验；从 LatencyDB 读取从 21000001 开始的真实 tx latency/rw，默认执行 10000 个区块。")
	fmt.Println()
	fmt.Println("Synthetic examples:")
	fmt.Println("  go run ./experiment -baseline janus -t 8 -b 10 -bt 2000")
	fmt.Println("  go run ./experiment synthetic -baseline all -t 8 -b 10 -bt 2000 -sk 0.5 -lr 0.5 -sr 0.5")
	fmt.Println()
	fmt.Println("Synthetic flags:")
	fmt.Println("  -baseline  all | harmony | schain | serial | optme | optme_paper | aria | janus | Non_Maximum_Commit_Validation | newHarmony")
	fmt.Println("             选择要运行的 baseline；默认 all。")
	fmt.Println("  -t         worker thread number；默认 8。")
	fmt.Println("  -b         blocks number；合成负载生成的区块数量，默认 10。")
	fmt.Println("  -bt        transactions per block；每个合成区块的交易数量，默认 2000。")
	fmt.Println("  -sk        zipf skew；控制合成交易地址热点/冲突倾斜度，默认 0.5。")
	fmt.Println("  -ar        address number rate；地址数量 = blockTxNumber * ar，默认 4。")
	fmt.Println("  -lr        long transaction rate；长交易比例，默认 0.5。")
	fmt.Println("  -sr        short transaction rate；短交易比例，默认 0.5。")
	fmt.Println("  -wa        water mark alpha；Janus 水位线 alpha，默认 1.5。")
	fmt.Println("  -wb        water mark beta；Janus 水位线 beta，默认 3.5。")
	fmt.Println("  -f         fibonacci number；-1 表示随机生成，默认 10。")
	fmt.Println("  -sfln      short transaction fibonacci loop number；默认 20。")
	fmt.Println("  -lfln      long transaction fibonacci loop number；默认 40。")
	fmt.Println("  -r         recursive calculate fibonacci；是否递归计算斐波那契，默认 false。")
	fmt.Println("  -ta        trace transaction abort；是否追踪丢弃交易，默认 false。")
	fmt.Println()
	fmt.Println("Ethereum real workload examples:")
	fmt.Println("  go run ./experiment ethereum -baseline janus -t 8 -b 10000 -latency 50")
	fmt.Println("  go run ./experiment ethereum -baseline all -t 8 -b 10000 -latency 50")
	fmt.Println()
	fmt.Println("Ethereum flags:")
	fmt.Println("  -baseline  all | harmony | schain | serial | optme | optme_paper | aria | janus | Non_Maximum_Commit_Validation | newHarmony")
	fmt.Println("             选择要运行的 baseline；默认 janus。ethereum 模式下 harmony 使用 new_harmony，newHarmony 仅作为兼容别名。")
	fmt.Println("  -t         worker thread number；默认 8。")
	fmt.Println("  -b         ethereum blocks number；从 21000001 开始连续执行的真实区块数量，默认 10000。")
	fmt.Println("  -latency   long/short threshold in microseconds；tx latency < threshold 为短交易，否则为长交易，默认 50。")
}
