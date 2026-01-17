package janus

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// MWISInput ILP 求解器的输入格式
type MWISInput struct {
    Nodes   []int              `json:"nodes"`
    Edges   [][2]int           `json:"edges"`
    Weights map[string]float64 `json:"weights"`
}

// MWISOutput ILP 求解器的输出格式
type MWISOutput struct {
    Status        string  `json:"status"`
    IndependentSet []int   `json:"independent_set"`
    TotalWeight   float64 `json:"total_weight"`
}

// SolveMWIS 求解单个连通分量的最大权重独立集
// nodes: 连通分量中的节点列表
// 返回: 最大权重独立集的节点列表
func SolveMWIS(dag *ConflictDAG, nodes []int) ([]int, error) {
    // 单节点直接返回
    if len(nodes) == 1 {
        return nodes, nil
    }

    // 准备输入数据
    input := MWISInput{
        Nodes:   nodes,
        Edges:   make([][2]int, 0),
        Weights: make(map[string]float64),
    }

    // 创建节点集合，用于查找
    nodeSet := make(map[int]bool)
    for _, n := range nodes {
        nodeSet[n] = true
    }

    // 收集边（只收集连通分量内部的边）
    edgeSet := make(map[string]bool) // 用于去重
    for _, nodeID := range nodes {
		if dag.Edges[nodeID] == nil {
            continue
        }

        for toNodeID := range dag.Edges[nodeID] {
            // 确保 toNodeID 也在连通分量中
            if !nodeSet[toNodeID] {
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
                input.Edges = append(input.Edges, [2]int{u, v})
            }
        }
    }

	// 如果没有边，所有节点都可以提交
    if len(input.Edges) == 0 {
        return nodes, nil
    }

    // 收集权重
    for _, nodeID := range nodes {
        if rwset, exists := dag.Nodes[nodeID]; exists {
            input.Weights[fmt.Sprintf("%d", nodeID)] = rwset.Cost
        } else {
            input.Weights[fmt.Sprintf("%d", nodeID)] = 1.0 // 默认权重
        }
    }

    // 调用 Python 求解器
    result, err := callPythonSolver(input)
    if err != nil {
        return nil, err
    }

    // 返回结果
    if result.Status != "optimal" {
        return nil, fmt.Errorf("求解失败: %s", result.Status)
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
    return "/root/Janus/januscore/janus"
}