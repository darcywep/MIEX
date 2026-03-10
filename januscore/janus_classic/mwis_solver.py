#!/usr/bin/env python3
"""
最大权重独立集求解器
使用 Google OR-Tools 的整数线性规划求解

输入: JSON 文件，包含节点、边、权重
输出: JSON 文件，包含独立集的节点列表
"""

import json
import sys
from ortools.linear_solver import pywraplp


def solve_mwis(input_file, output_file):
    """
    求解最大权重独立集
    
    输入文件格式：
    {
        "nodes": [1, 2, 14, 19],           // 节点ID列表
        "edges": [[1,2], [1,14], [2,19]],  // 边列表
        "weights": {                        // 节点权重
            "1": 100.5,
            "2": 50.0,
            "14": 80.0,
            "19": 120.0
        }
    }
    
    输出文件格式：
    {
        "independent_set": [2, 19],        // 独立集的节点
        "total_weight": 170.0              // 总权重
    }
    """
    
    # 1. 读取输入
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    nodes = data['nodes']
    edges = data['edges']
    weights = data['weights']
    
    # 2. 创建求解器
    solver = pywraplp.Solver.CreateSolver('SCIP')
    if not solver:
        print("无法创建求解器", file=sys.stderr)
        sys.exit(1)
    
    # 3. 创建变量：x[i] ∈ {0, 1}，表示是否选择节点 i
    x = {}
    for node in nodes:
        x[node] = solver.IntVar(0, 1, f'x_{node}')
    
    # 4. 添加约束：对于每条边 (u, v)，x[u] + x[v] <= 1
    for edge in edges:
        u, v = edge[0], edge[1]
        solver.Add(x[u] + x[v] <= 1)
    
    # 5. 设置目标函数：最大化 Σ weight[i] * x[i]
    objective = solver.Objective()
    for node in nodes:
        # 权重字典的 key 是字符串
        weight = weights.get(str(node), 1.0)
        objective.SetCoefficient(x[node], weight)
    objective.SetMaximization()
    
    # 6. 求解
    status = solver.Solve()
    
    # 7. 输出结果
    if status == pywraplp.Solver.OPTIMAL:
        independent_set = []
        total_weight = 0.0
        
        for node in nodes:
            if x[node].solution_value() > 0.5:  # 选中
                independent_set.append(node)
                total_weight += weights.get(str(node), 1.0)
        
        result = {
            'status': 'optimal',
            'independent_set': independent_set,
            'total_weight': total_weight
        }
    else:
        result = {
            'status': 'failed',
            'independent_set': [],
            'total_weight': 0.0
        }
    
    # 8. 写入输出文件
    with open(output_file, 'w') as f:
        json.dump(result, f)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(f"用法: {sys.argv[0]} <输入文件> <输出文件>", file=sys.stderr)
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    solve_mwis(input_file, output_file)