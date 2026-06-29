package janus

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"time"
)

// MWISSolver 求解器类型
type MWISSolver int

const (
	SolverILP          MWISSolver = iota // 使用内置精确 MWIS 求解
	SolverGreedy                         // 使用贪心算法求解
	SolverCostOnly                       // 只按执行开销贪心求解
	SolverLPRelaxation                   // 使用 LP relaxation 后 rounding 求解
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

// MWISInput Python 求解器的输入格式
type MWISInput struct {
	Nodes   []int              `json:"nodes"`
	Edges   [][2]int           `json:"edges"`
	Weights map[string]float64 `json:"weights"`
}

// MWISOutput Python 求解器的输出格式
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
	case SolverCostOnly:
		return solveByCostOnly(dag, nodes, subgraph)
	case SolverLPRelaxation:
		return solveByLPRelaxation(dag, nodes, subgraph)
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

// solveByCostOnly 使用只考虑执行开销的贪心算法求解最大权重独立集。
// 策略：按交易执行开销从大到小选择节点，不使用冲突度数作为排序因子。
func solveByCostOnly(dag *ConflictDAG, nodes []int, info *subgraphInfo) ([]int, error) {
	nodeList := make([]*nodeWithWeight, 0, len(nodes))
	for _, nodeID := range nodes {
		weight := info.weights[nodeID]
		nodeList = append(nodeList, &nodeWithWeight{
			nodeID: nodeID,
			weight: weight,
			degree: len(info.adjList[nodeID]),
			score:  weight,
		})
	}

	sort.Slice(nodeList, func(i, j int) bool {
		if nodeList[i].weight != nodeList[j].weight {
			return nodeList[i].weight > nodeList[j].weight
		}
		return nodeList[i].nodeID < nodeList[j].nodeID
	})

	result := selectIndependentSet(nodeList, info)
	if !isIndependentSet(result, info) {
		result = repairIndependentSetByOrder(result, nodes, info)
	}
	totalWeight := totalIndependentSetWeight(result, info)
	if enableLog {
		fmt.Printf("[CostOnly] 节点数=%d, 边数=%d, 独立集大小=%d, 总权重=%.2f\n",
			len(nodes), len(info.edges), len(result), totalWeight)
	}

	return result, nil
}

func selectIndependentSet(nodeList []*nodeWithWeight, info *subgraphInfo) []int {
	excluded := make(map[int]bool)
	result := make([]int, 0)

	for _, node := range nodeList {
		nodeID := node.nodeID
		if excluded[nodeID] {
			continue
		}

		result = append(result, nodeID)
		for neighbor := range info.adjList[nodeID] {
			excluded[neighbor] = true
		}
	}

	return result
}

func totalIndependentSetWeight(independentSet []int, info *subgraphInfo) float64 {
	totalWeight := 0.0
	for _, nodeID := range independentSet {
		totalWeight += info.weights[nodeID]
	}
	return totalWeight
}

func isIndependentSet(independentSet []int, info *subgraphInfo) bool {
	selected := make(map[int]struct{}, len(independentSet))
	for _, nodeID := range independentSet {
		if !info.nodeSet[nodeID] {
			return false
		}
		if _, exists := selected[nodeID]; exists {
			return false
		}
		for neighbor := range info.adjList[nodeID] {
			if _, exists := selected[neighbor]; exists {
				return false
			}
		}
		selected[nodeID] = struct{}{}
	}
	return true
}

func repairIndependentSetByOrder(order []int, nodes []int, info *subgraphInfo) []int {
	nodeList := make([]*nodeWithWeight, 0, len(nodes))
	seen := make(map[int]bool, len(nodes))
	for _, nodeID := range order {
		if !info.nodeSet[nodeID] || seen[nodeID] {
			continue
		}
		seen[nodeID] = true
		nodeList = append(nodeList, &nodeWithWeight{
			nodeID: nodeID,
			weight: info.weights[nodeID],
			degree: len(info.adjList[nodeID]),
			score:  info.weights[nodeID],
		})
	}

	missing := make([]*nodeWithWeight, 0)
	for _, nodeID := range nodes {
		if seen[nodeID] {
			continue
		}
		missing = append(missing, &nodeWithWeight{
			nodeID: nodeID,
			weight: info.weights[nodeID],
			degree: len(info.adjList[nodeID]),
			score:  info.weights[nodeID],
		})
	}
	sort.Slice(missing, func(i, j int) bool {
		if missing[i].weight != missing[j].weight {
			return missing[i].weight > missing[j].weight
		}
		return missing[i].nodeID < missing[j].nodeID
	})
	nodeList = append(nodeList, missing...)

	return selectIndependentSet(nodeList, info)
}

// ===================== 精确 MWIS 求解器 =====================

type exactMWISResult struct {
	weight float64
	mask   *big.Int
}

// solveByILP 保留历史命名，实际使用 Go 内置分支限界精确求解最大权重独立集。
// 这样 oracle baseline 不依赖 Python/OR-Tools；外部求解器失败时不会把整块冲突分量都 abort。
func solveByILP(dag *ConflictDAG, nodes []int, info *subgraphInfo) ([]int, error) {
	independentSet, totalWeight := solveExactMWISNative(nodes, info)
	if !isIndependentSet(independentSet, info) {
		return nil, fmt.Errorf("exact MWIS returned conflicting set: %v", independentSet)
	}

	if enableLog {
		fmt.Printf("[ExactMWIS] 节点数=%d, 边数=%d, 独立集大小=%d, 总权重=%.2f\n",
			len(nodes), len(info.edges), len(independentSet), totalWeight)
	}

	return independentSet, nil
}

func solveExactMWISNative(nodes []int, info *subgraphInfo) ([]int, float64) {
	orderedNodes := append([]int(nil), nodes...)
	sort.Slice(orderedNodes, func(i, j int) bool {
		iNode, jNode := orderedNodes[i], orderedNodes[j]
		iDegree, jDegree := len(info.adjList[iNode]), len(info.adjList[jNode])
		if iDegree != jDegree {
			return iDegree > jDegree
		}
		if info.weights[iNode] != info.weights[jNode] {
			return info.weights[iNode] > info.weights[jNode]
		}
		return iNode < jNode
	})

	indexByNode := make(map[int]int, len(orderedNodes))
	for idx, nodeID := range orderedNodes {
		indexByNode[nodeID] = idx
	}

	weights := make([]float64, len(orderedNodes))
	adjacency := make([]*big.Int, len(orderedNodes))
	for idx, nodeID := range orderedNodes {
		weights[idx] = info.weights[nodeID]
		adjacency[idx] = new(big.Int)
		for neighbor := range info.adjList[nodeID] {
			if neighborIdx, ok := indexByNode[neighbor]; ok {
				adjacency[idx].SetBit(adjacency[idx], neighborIdx, 1)
			}
		}
	}

	allCandidates := new(big.Int)
	for idx := range orderedNodes {
		allCandidates.SetBit(allCandidates, idx, 1)
	}

	memo := make(map[string]exactMWISResult)
	result := solveExactMWISRecursive(allCandidates, adjacency, weights, orderedNodes, memo)

	independentSet := make([]int, 0, result.mask.BitLen())
	for idx := range orderedNodes {
		if result.mask.Bit(idx) != 0 {
			independentSet = append(independentSet, orderedNodes[idx])
		}
	}
	sort.Ints(independentSet)

	return independentSet, result.weight
}

func solveExactMWISRecursive(candidates *big.Int, adjacency []*big.Int, weights []float64, orderedNodes []int, memo map[string]exactMWISResult) exactMWISResult {
	if candidates.Sign() == 0 {
		return exactMWISResult{mask: new(big.Int)}
	}

	key := candidates.Text(16)
	if cached, ok := memo[key]; ok {
		return cloneExactMWISResult(cached)
	}

	branchVertex, maxDegree := chooseExactMWISBranchVertex(candidates, adjacency, weights, orderedNodes)
	if maxDegree == 0 {
		result := exactMWISResult{
			weight: candidateWeight(candidates, weights),
			mask:   new(big.Int).Set(candidates),
		}
		memo[key] = cloneExactMWISResult(result)
		return result
	}

	withoutBranchVertex := new(big.Int).Set(candidates)
	withoutBranchVertex.SetBit(withoutBranchVertex, branchVertex, 0)

	includeCandidates := new(big.Int).AndNot(withoutBranchVertex, adjacency[branchVertex])
	includeResult := solveExactMWISRecursive(includeCandidates, adjacency, weights, orderedNodes, memo)
	includeResult.weight += weights[branchVertex]
	includeResult.mask.SetBit(includeResult.mask, branchVertex, 1)

	excludeResult := solveExactMWISRecursive(withoutBranchVertex, adjacency, weights, orderedNodes, memo)

	result := excludeResult
	if includeResult.weight > excludeResult.weight+1e-9 ||
		(math.Abs(includeResult.weight-excludeResult.weight) <= 1e-9 && compareExactMWISMasks(includeResult.mask, excludeResult.mask, orderedNodes) < 0) {
		result = includeResult
	}

	memo[key] = cloneExactMWISResult(result)
	return result
}

func chooseExactMWISBranchVertex(candidates *big.Int, adjacency []*big.Int, weights []float64, orderedNodes []int) (int, int) {
	bestIdx := -1
	bestDegree := -1
	neighbors := new(big.Int)
	for idx := range orderedNodes {
		if candidates.Bit(idx) == 0 {
			continue
		}

		neighbors.And(adjacency[idx], candidates)
		degree := countSetBits(neighbors, len(orderedNodes))
		if degree > bestDegree ||
			(degree == bestDegree && weights[idx] > weights[bestIdx]) ||
			(degree == bestDegree && weights[idx] == weights[bestIdx] && orderedNodes[idx] < orderedNodes[bestIdx]) {
			bestIdx = idx
			bestDegree = degree
		}
	}

	return bestIdx, bestDegree
}

func candidateWeight(candidates *big.Int, weights []float64) float64 {
	total := 0.0
	for idx := range weights {
		if candidates.Bit(idx) != 0 {
			total += weights[idx]
		}
	}
	return total
}

func countSetBits(mask *big.Int, limit int) int {
	count := 0
	for idx := 0; idx < limit; idx++ {
		if mask.Bit(idx) != 0 {
			count++
		}
	}
	return count
}

func cloneExactMWISResult(result exactMWISResult) exactMWISResult {
	return exactMWISResult{
		weight: result.weight,
		mask:   new(big.Int).Set(result.mask),
	}
}

func compareExactMWISMasks(left, right *big.Int, orderedNodes []int) int {
	for idx := range orderedNodes {
		leftHas := left.Bit(idx) != 0
		rightHas := right.Bit(idx) != 0
		if leftHas == rightHas {
			continue
		}
		if leftHas {
			return -1
		}
		return 1
	}
	return 0
}

// solveByLPRelaxation 使用 LP relaxation 解和确定性 rounding 近似求解最大权重独立集。
func solveByLPRelaxation(dag *ConflictDAG, nodes []int, info *subgraphInfo) ([]int, error) {
	input := MWISInput{
		Nodes:   nodes,
		Edges:   info.edges,
		Weights: make(map[string]float64),
	}

	for nodeID, weight := range info.weights {
		input.Weights[fmt.Sprintf("%d", nodeID)] = weight
	}

	result, err := callPythonSolver(input, "lp_relaxation")
	if err != nil {
		return nil, err
	}

	if result.Status != "optimal" {
		return nil, fmt.Errorf("LP relaxation 求解失败: %s", result.Status)
	}
	independentSet := result.IndependentSet
	if !isIndependentSet(independentSet, info) {
		independentSet = repairIndependentSetByOrder(result.IndependentSet, nodes, info)
	}

	if enableLog {
		fmt.Printf("[LPRelaxation] 节点数=%d, 边数=%d, 独立集大小=%d, 总权重=%.2f\n",
			len(nodes), len(info.edges), len(independentSet), totalIndependentSetWeight(independentSet, info))
	}

	return independentSet, nil
}

// callPythonSolver 调用 Python 脚本求解 LP relaxation；精确 MWIS 已改为 Go 内置实现。
func callPythonSolver(input MWISInput, mode string) (*MWISOutput, error) {
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
	pythonBin := os.Getenv("JANUS_PYTHON")
	if pythonBin == "" {
		pythonBin = "python3"
	}
	cmd := exec.Command(pythonBin, scriptPath, inputFile.Name(), outputFile.Name(), mode)
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
	if _, filename, _, ok := runtime.Caller(0); ok {
		return filepath.Dir(filename)
	}
	if cwd, err := os.Getwd(); err == nil {
		return cwd
	}
	return "."
}

// SetMWISSolver 设置求解器类型
func SetMWISSolver(solver MWISSolver) {
	DefaultMWISSolver = solver
	switch solver {
	case SolverILP:
		fmt.Println("[MWIS] 使用内置精确 MWIS 求解器")
	case SolverGreedy:
		fmt.Println("[MWIS] 使用贪心算法求解器")
	case SolverCostOnly:
		fmt.Println("[MWIS] 使用只考虑执行开销的贪心求解器")
	case SolverLPRelaxation:
		fmt.Println("[MWIS] 使用 LP relaxation 求解器")
	}
}

// GetMWISSolverName 获取当前求解器名称
func GetMWISSolverName() string {
	switch DefaultMWISSolver {
	case SolverILP:
		return "ExactMWIS"
	case SolverGreedy:
		return "Greedy"
	case SolverCostOnly:
		return "CostOnly"
	case SolverLPRelaxation:
		return "LPRelaxation"
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
