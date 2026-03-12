package janusClassicOCC

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"
)

// MWISSolver 求解器类型
type MWISSolver int

const (
	SolverILP    MWISSolver = iota // 使用 ILP (OR-Tools) 求解
	SolverGreedy                   // 使用贪心算法求解
)

// 默认求解器
var DefaultMWISSolver = SolverGreedy

var EnableMWISBenchmark = false

func SetMWISBenchmark(enable bool) {
	EnableMWISBenchmark = enable
	if enable {
		fmt.Println("[MWIS] 性能对比测试")
	}
}

// MWISInput ILP 求解器的输入格式
type MWISInput struct {
	Nodes   []int              `json:"nodes"`
	Edges   [][2]int           `json:"edges"`
	Weights map[string]float64 `json:"weights"`
}

// MWISOutput ILP 求解器的输出格式
type MWISOutput struct {
	Status         string  `json:"status"`
	IndependentSet []int   `json:"independent_set"`
	TotalWeight    float64 `json:"total_weight"`
}

// SolveMWIS 求解单个连通分量的最大权重独立集
// 根据 DefaultMWISSolver 选择使用 ILP 或贪心算法
func SolveMWIS(dag *ConflictDAG, nodes []int) ([]int, error) {
	// 单节点直接返回
	if len(nodes) == 1 {
		return nodes, nil
	}

	// 提取子图信息
	subgraph := extractSubgraph(dag, nodes)

	// 如果没有边，所有节点都可以提交
	if len(subgraph.edges) == 0 {
		return nodes, nil
	}

	// 根据配置选择求解器
	switch DefaultMWISSolver {
	case SolverILP:
		return solveByILP(dag, nodes, subgraph)
	case SolverGreedy:
		return solveByGreedy(dag, nodes, subgraph)
	default:
		return solveByGreedy(dag, nodes, subgraph)
	}
}

// subgraphInfo 子图信息
type subgraphInfo struct {
	nodeSet map[int]bool         // 节点集合
	edges   [][2]int             // 边列表
	weights map[int]float64      // 节点权重
	adjList map[int]map[int]bool // 邻接表
}

// extractSubgraph 从 DAG 中提取指定节点的子图信息
func extractSubgraph(dag *ConflictDAG, nodes []int) *subgraphInfo {
	info := &subgraphInfo{
		nodeSet: make(map[int]bool),
		edges:   make([][2]int, 0),
		weights: make(map[int]float64),
		adjList: make(map[int]map[int]bool),
	}

	// 构建节点集合
	for _, n := range nodes {
		info.nodeSet[n] = true
		info.adjList[n] = make(map[int]bool)
	}

	// 收集边和邻接表
	edgeSet := make(map[string]bool)
	for _, nodeID := range nodes {
		if dag.Edges[nodeID] == nil {
			continue
		}
		for toNodeID := range dag.Edges[nodeID] {
			if !info.nodeSet[toNodeID] {
				continue
			}
			// 边去重：只保留 (小, 大) 形式
			u, v := nodeID, toNodeID
			if u > v {
				u, v = v, u
			}
			edgeKey := fmt.Sprintf("%d-%d", u, v)
			if !edgeSet[edgeKey] {
				edgeSet[edgeKey] = true
				info.edges = append(info.edges, [2]int{u, v})
			}
			// 邻接表
			info.adjList[nodeID][toNodeID] = true
			info.adjList[toNodeID][nodeID] = true
		}
	}

	// 收集权重
	for _, nodeID := range nodes {
		if rwset, exists := dag.Nodes[nodeID]; exists && rwset != nil {
			info.weights[nodeID] = rwset.Cost
		} else {
			info.weights[nodeID] = 1.0
		}
	}

	return info
}

// ===================== 贪心算法 =====================

// nodeWithWeight 用于排序的节点结构
type nodeWithWeight struct {
	nodeID int
	weight float64
	degree int
	score  float64 // 性价比 = weight / (degree + 1)
}

// solveByGreedy 使用贪心算法求解最大权重独立集
// 策略：按 "权重/度数" 的性价比从高到低选择节点
func solveByGreedy(dag *ConflictDAG, nodes []int, info *subgraphInfo) ([]int, error) {
	// 1. 计算每个节点的度数和性价比
	nodeList := make([]*nodeWithWeight, 0, len(nodes))
	for _, nodeID := range nodes {
		degree := len(info.adjList[nodeID])
		weight := info.weights[nodeID]
		score := weight / float64(degree+1) // +1 避免除以0

		nodeList = append(nodeList, &nodeWithWeight{
			nodeID: nodeID,
			weight: weight,
			degree: degree,
			score:  score,
		})
	}

	// 2. 按性价比降序排列
	sort.Slice(nodeList, func(i, j int) bool {
		return nodeList[i].score > nodeList[j].score
	})

	// 3. 贪心选择
	selected := make(map[int]bool) // 已选中的节点
	excluded := make(map[int]bool) // 被排除的节点（与已选节点相邻）
	result := make([]int, 0)

	for _, node := range nodeList {
		nodeID := node.nodeID

		// 如果该节点已被排除，跳过
		if excluded[nodeID] {
			continue
		}

		// 选中该节点
		selected[nodeID] = true
		result = append(result, nodeID)

		// 排除所有邻居节点
		for neighbor := range info.adjList[nodeID] {
			excluded[neighbor] = true
		}
	}

	// 计算总权重（用于日志）
	totalWeight := 0.0
	for _, nodeID := range result {
		totalWeight += info.weights[nodeID]
	}
	if enableLog {
		fmt.Printf("[Greedy] 节点数=%d, 边数=%d, 独立集大小=%d, 总权重=%.2f\n",
			len(nodes), len(info.edges), len(result), totalWeight)
	}

	return result, nil
}

// ===================== ILP 求解器 =====================

// solveByILP 使用 ILP (OR-Tools) 求解最大权重独立集
func solveByILP(dag *ConflictDAG, nodes []int, info *subgraphInfo) ([]int, error) {
	// 1. 准备输入数据
	input := MWISInput{
		Nodes:   nodes,
		Edges:   info.edges,
		Weights: make(map[string]float64),
	}

	// 转换权重格式（key 需要是字符串）
	for nodeID, weight := range info.weights {
		input.Weights[fmt.Sprintf("%d", nodeID)] = weight
	}

	// 2. 调用 Python 求解器
	result, err := callPythonSolver(input)
	if err != nil {
		return nil, err
	}

	// 3. 返回结果
	if result.Status != "optimal" {
		return nil, fmt.Errorf("ILP 求解失败: %s", result.Status)
	}

	if enableLog {
		fmt.Printf("[ILP] 节点数=%d, 边数=%d, 独立集大小=%d, 总权重=%.2f\n",
			len(nodes), len(info.edges), len(result.IndependentSet), result.TotalWeight)
	}

	return result.IndependentSet, nil
}

// callPythonSolver 调用 Python 脚本求解 ILP
func callPythonSolver(input MWISInput) (*MWISOutput, error) {
	// 创建临时文件
	inputFile, err := os.CreateTemp("", "mwis_input_*.json")
	if err != nil {
		return nil, fmt.Errorf("创建输入文件失败: %v", err)
	}
	defer os.Remove(inputFile.Name())

	outputFile, err := os.CreateTemp("", "mwis_output_*.json")
	if err != nil {
		return nil, fmt.Errorf("创建输出文件失败: %v", err)
	}
	defer os.Remove(outputFile.Name())
	outputFile.Close() // 先关闭，让 Python 写入

	// 写入输入数据
	encoder := json.NewEncoder(inputFile)
	if err := encoder.Encode(input); err != nil {
		return nil, fmt.Errorf("写入输入数据失败: %v", err)
	}
	inputFile.Close()

	// 获取 Python 脚本路径（假设在同一目录）
	scriptPath := filepath.Join(getScriptDir(), "mwis_solver.py")

	// 调用 Python 脚本
	cmd := exec.Command("python3", scriptPath, inputFile.Name(), outputFile.Name())
	cmdOutput, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("调用 Python 脚本失败: %v, 输出: %s", err, string(cmdOutput))
	}

	// 读取输出结果
	outputData, err := os.ReadFile(outputFile.Name())
	if err != nil {
		return nil, fmt.Errorf("读取输出文件失败: %v", err)
	}

	var result MWISOutput
	if err := json.Unmarshal(outputData, &result); err != nil {
		return nil, fmt.Errorf("解析输出数据失败: %v", err)
	}

	return &result, nil
}

// getScriptDir 获取脚本所在目录
func getScriptDir() string {
	// 返回 mwis_solver.py 所在的目录
	// 你可能需要根据实际部署情况调整这个路径
	return "/root/Janus/januscore/janus"
}

// SetMWISSolver 设置求解器类型
func SetMWISSolver(solver MWISSolver) {
	DefaultMWISSolver = solver
	switch solver {
	case SolverILP:
		fmt.Println("[MWIS] 使用 ILP (OR-Tools) 求解器")
	case SolverGreedy:
		fmt.Println("[MWIS] 使用贪心算法求解器")
	}
}

// GetMWISSolverName 获取当前求解器名称
func GetMWISSolverName() string {
	switch DefaultMWISSolver {
	case SolverILP:
		return "ILP"
	case SolverGreedy:
		return "Greedy"
	default:
		return "Unknown"
	}
}

// BenchmarkResult 单次算法求解结果
type BenchmarkResult struct {
	SolverName     string        // 求解器名称
	TotalTime      time.Duration // 总求解时间
	TotalWeight    float64       // 总权重
	CommitCount    int           // 提交交易数
	AbortCount     int           // 中止交易数
	ComponentCount int           // 连通分量数
}

// BenchmarkMWIS 对同一批数据用两种算法进行性能对比
// 返回 Greedy 结果和 ILP 结果
func BenchmarkMWIS(dag *ConflictDAG) (*BenchmarkResult, *BenchmarkResult) {
	// 获取所有连通分量
	components := dag.GetConnectedComponents()

	// 转换为列表形式
	componentList := make([][]int, 0, len(components))
	for _, nodes := range components {
		componentList = append(componentList, nodes)
	}

	totalNodes := len(dag.Nodes)

	fmt.Println("\n[Benchmark] 开始性能对比测试...")

	// 1. 测试 Greedy 算法
	fmt.Println("[Benchmark] 测试 Greedy 算法...")
	greedyResult := benchmarkSolver(dag, componentList, totalNodes, "Greedy", solveByGreedy)

	time.Sleep(3 * time.Second)
	// 2. 测试 ILP 算法
	fmt.Println("[Benchmark] 测试 ILP 算法...")
	ilpResult := benchmarkSolver(dag, componentList, totalNodes, "ILP", solveByILP)

	// 3. 打印对比报告
	printBenchmarkReport(greedyResult, ilpResult)

	return greedyResult, ilpResult
}

// benchmarkSolver 测试单个求解器的性能
func benchmarkSolver(
	dag *ConflictDAG,
	componentList [][]int,
	totalNodes int,
	solverName string,
	solver func(*ConflictDAG, []int, *subgraphInfo) ([]int, error),
) *BenchmarkResult {
	result := &BenchmarkResult{
		SolverName:     solverName,
		ComponentCount: len(componentList),
	}

	commitSet := make(map[int]bool)
	startTime := time.Now()

	for _, nodes := range componentList {
		if len(nodes) == 1 {
			// 单节点直接提交
			commitSet[nodes[0]] = true
			if rwset, exists := dag.Nodes[nodes[0]]; exists && rwset != nil {
				result.TotalWeight += rwset.Cost
			}
			continue
		}

		// 提取子图信息
		subgraph := extractSubgraph(dag, nodes)

		// 如果没有边，所有节点都可以提交
		if len(subgraph.edges) == 0 {
			for _, nodeID := range nodes {
				commitSet[nodeID] = true
				if rwset, exists := dag.Nodes[nodeID]; exists && rwset != nil {
					result.TotalWeight += rwset.Cost
				}
			}
			continue
		}

		// 调用求解器
		independentSet, err := solver(dag, nodes, subgraph)
		if err != nil {
			fmt.Printf("[%s] 求解失败: %v\n", solverName, err)
			continue
		}

		// 统计结果
		for _, nodeID := range independentSet {
			commitSet[nodeID] = true
			if rwset, exists := dag.Nodes[nodeID]; exists && rwset != nil {
				result.TotalWeight += rwset.Cost
			}
		}
	}

	result.TotalTime = time.Since(startTime)
	result.CommitCount = len(commitSet)
	result.AbortCount = totalNodes - result.CommitCount

	return result
}

// printBenchmarkReport 打印性能对比报告
func printBenchmarkReport(greedy, ilp *BenchmarkResult) {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║              MWIS 算法性能对比报告                               ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ 连通分量总数:                    %-30d ║\n", greedy.ComponentCount)
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")
	fmt.Println("║                        Greedy          ILP            对比       ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")

	// 求解时间
	speedup := float64(ilp.TotalTime) / float64(greedy.TotalTime)
	fmt.Printf("║ 求解时间:          %12v    %12v    %.1fx faster  ║\n",
		greedy.TotalTime.Round(time.Microsecond),
		ilp.TotalTime.Round(time.Microsecond),
		speedup)

	// 总权重
	weightRatio := greedy.TotalWeight / ilp.TotalWeight * 100
	fmt.Printf("║ 总权重:            %12.2f    %12.2f    %.1f%%        ║\n",
		greedy.TotalWeight, ilp.TotalWeight, weightRatio)

	// 提交交易数
	fmt.Printf("║ 提交交易数:        %12d    %12d                 ║\n",
		greedy.CommitCount, ilp.CommitCount)
	// 中止交易数
	fmt.Printf("║ 中止交易数:        %12d    %12d                 ║\n",
		greedy.AbortCount, ilp.AbortCount)

	// 提交率
	greedyCommitRate := float64(greedy.CommitCount) / float64(greedy.CommitCount+greedy.AbortCount) * 100
	ilpCommitRate := float64(ilp.CommitCount) / float64(ilp.CommitCount+ilp.AbortCount) * 100
	fmt.Printf("║ 提交率:            %11.1f%%    %11.1f%%                 ║\n",
		greedyCommitRate, ilpCommitRate)

	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")

	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
	fmt.Println()
}
