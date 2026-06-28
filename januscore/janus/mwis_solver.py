#!/usr/bin/env python3
"""MWIS solver helpers for Janus validation experiments.

Usage:
  mwis_solver.py <input.json> <output.json> [ilp|lp_relaxation]

The ILP mode preserves the original exact OR-Tools formulation. The
lp_relaxation mode solves the continuous relaxation and then deterministically
rounds the fractional solution into a valid independent set.
"""

import json
import sys
from ortools.linear_solver import pywraplp


OPTIMAL_STATUS = "optimal"
FAILED_STATUS = "failed"


def create_solver(names):
    for name in names:
        solver = pywraplp.Solver.CreateSolver(name)
        if solver:
            return solver
    return None


def load_input(input_file):
    with open(input_file, "r") as f:
        data = json.load(f)

    nodes = data["nodes"]
    edges = data["edges"]
    weights = data["weights"]
    return nodes, edges, weights


def write_output(output_file, result):
    with open(output_file, "w") as f:
        json.dump(result, f)


def solve_ilp(nodes, edges, weights):
    solver = create_solver(("SCIP", "CBC_MIXED_INTEGER_PROGRAMMING", "CBC"))
    if not solver:
        raise RuntimeError("unable to create an OR-Tools ILP solver")

    x = {node: solver.IntVar(0, 1, f"x_{node}") for node in nodes}

    for edge in edges:
        u, v = edge[0], edge[1]
        solver.Add(x[u] + x[v] <= 1)

    objective = solver.Objective()
    for node in nodes:
        objective.SetCoefficient(x[node], weights.get(str(node), 1.0))
    objective.SetMaximization()

    status = solver.Solve()
    if status != pywraplp.Solver.OPTIMAL:
        return {
            "status": FAILED_STATUS,
            "independent_set": [],
            "total_weight": 0.0,
        }

    independent_set = []
    total_weight = 0.0
    for node in nodes:
        if x[node].solution_value() > 0.5:
            independent_set.append(node)
            total_weight += weights.get(str(node), 1.0)

    return {
        "status": OPTIMAL_STATUS,
        "independent_set": independent_set,
        "total_weight": total_weight,
    }


def solve_lp_relaxation(nodes, edges, weights):
    solver = create_solver(("GLOP", "CLP"))
    if not solver:
        raise RuntimeError("unable to create an OR-Tools LP solver")

    x = {node: solver.NumVar(0.0, 1.0, f"x_{node}") for node in nodes}

    adjacency = {node: set() for node in nodes}
    for edge in edges:
        u, v = edge[0], edge[1]
        solver.Add(x[u] + x[v] <= 1.0)
        adjacency[u].add(v)
        adjacency[v].add(u)

    objective = solver.Objective()
    for node in nodes:
        objective.SetCoefficient(x[node], weights.get(str(node), 1.0))
    objective.SetMaximization()

    status = solver.Solve()
    if status != pywraplp.Solver.OPTIMAL:
        return {
            "status": FAILED_STATUS,
            "independent_set": [],
            "total_weight": 0.0,
            "lp_objective": 0.0,
        }

    fractional_values = {node: x[node].solution_value() for node in nodes}
    order = sorted(
        nodes,
        key=lambda node: (
            -fractional_values[node],
            -weights.get(str(node), 1.0),
            len(adjacency[node]),
            node,
        ),
    )

    selected = set()
    independent_set = []
    total_weight = 0.0
    for node in order:
        if any(neighbor in selected for neighbor in adjacency[node]):
            continue
        selected.add(node)
        independent_set.append(node)
        total_weight += weights.get(str(node), 1.0)

    return {
        "status": OPTIMAL_STATUS,
        "independent_set": independent_set,
        "total_weight": total_weight,
        "lp_objective": objective.Value(),
        "fractional_values": {str(k): v for k, v in fractional_values.items()},
    }


def solve_mwis(input_file, output_file, mode):
    nodes, edges, weights = load_input(input_file)

    if mode == "ilp":
        result = solve_ilp(nodes, edges, weights)
    elif mode in ("lp_relaxation", "lp-relaxation", "lp"):
        result = solve_lp_relaxation(nodes, edges, weights)
    else:
        raise ValueError(f"unknown solver mode: {mode}")

    write_output(output_file, result)


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print(
            f"Usage: {sys.argv[0]} <input.json> <output.json> [ilp|lp_relaxation]",
            file=sys.stderr,
        )
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    mode = sys.argv[3] if len(sys.argv) == 4 else "ilp"

    try:
        solve_mwis(input_file, output_file, mode)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
