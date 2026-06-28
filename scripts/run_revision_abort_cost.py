#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path


DEFAULT_METHODS = (
    "janus",
    "optme",
    "aria",
    "newHarmony",
    "janus_cost_only",
    "janus_lp_relaxation",
)
DEFAULT_SKEWS = "0.4,0.6,0.8,1.0,1.2"
DEFAULT_LONG_LOOPS = "20,30,40,50,60"

NUMBER_RE = r"([0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)"
SUMMARY_ROW_RE = re.compile(
    rf"^\s*(?P<method>[A-Za-z0-9_]+)\s+(?P<tps>{NUMBER_RE}|N/A)\s+(?P<latency>{NUMBER_RE}|N/A)\s*$"
)
BASELINE_RE = re.compile(r"^Baseline is\.\.\.\s*(?P<method>\S+)\s*$")
ABORT_COUNT_RE = re.compile(r"Number of abort transaction:\s*(?P<count>\d+)")
ABORT_COST_RE = re.compile(rf"Cost of abort transactions:\s*(?P<cost>{NUMBER_RE})")

CSV_COLUMNS = (
    "experiment",
    "case_id",
    "trial",
    "method",
    "long_rate",
    "short_rate",
    "skew",
    "fibonacci_n",
    "short_fibonacci_loop",
    "long_fibonacci_loop",
    "abort_count",
    "abort_cost",
    "tps",
    "latency_seconds",
    "status",
    "return_code",
    "duration_seconds",
    "log_file",
    "command",
)


@dataclass(frozen=True)
class ExperimentCase:
    experiment: str
    case_id: str
    skew: str
    fibonacci_n: int
    short_fibonacci_loop: int | None
    long_fibonacci_loop: int | None


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_int_list(value: str) -> list[int]:
    values: list[int] = []
    for item in parse_csv_list(value):
        values.append(int(item))
    if not values:
        raise argparse.ArgumentTypeError("list must not be empty")
    return values


def parse_skew_list(value: str) -> list[str]:
    skews = parse_csv_list(value)
    if not skews:
        raise argparse.ArgumentTypeError("skew list must not be empty")
    for skew in skews:
        float(skew)
    return skews


def parse_rate(value: str) -> float:
    token = value.strip()
    if token.endswith("%"):
        rate = float(token[:-1]) / 100.0
    else:
        rate = float(token)
        if rate > 1:
            rate = rate / 100.0
    if rate < 0 or rate > 1:
        raise argparse.ArgumentTypeError(f"rate must be in [0, 1]: {value}")
    return rate


def resolve_path(path: str | Path) -> Path:
    output = Path(path).expanduser()
    if not output.is_absolute():
        output = repo_root() / output
    return output


def sanitize_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")


def format_float(value: float) -> str:
    return f"{value:.10g}"


def build_cases(args: argparse.Namespace) -> list[ExperimentCase]:
    cases: list[ExperimentCase] = []
    for skew in args.skews:
        cases.append(
            ExperimentCase(
                experiment="skew_sweep",
                case_id=f"skew_{sanitize_filename(skew.replace('.', 'p'))}",
                skew=skew,
                fibonacci_n=-1,
                short_fibonacci_loop=None,
                long_fibonacci_loop=None,
            )
        )
    for long_loop in args.long_fibonacci_loops:
        cases.append(
            ExperimentCase(
                experiment="long_fibonacci_loop_sweep",
                case_id=f"skew_0p8_sfln_{args.loop_sweep_short_fibonacci_loop}_lfln_{long_loop}",
                skew="0.8",
                fibonacci_n=-1,
                short_fibonacci_loop=args.loop_sweep_short_fibonacci_loop,
                long_fibonacci_loop=long_loop,
            )
        )
    return cases


def build_env(args: argparse.Namespace, case: ExperimentCase) -> dict[str, str]:
    env = os.environ.copy()
    if args.go_cache.lower() != "default":
        go_cache = resolve_path(args.go_cache)
        go_cache.mkdir(parents=True, exist_ok=True)
        env["GOCACHE"] = str(go_cache)
    if args.monitor_dir.lower() != "default":
        monitor_root = resolve_path(args.monitor_dir)
        monitor_path = monitor_root / case.experiment / case.case_id
        monitor_path.mkdir(parents=True, exist_ok=True)
        env["JANUS_MONITOR_BASE_PATH"] = str(monitor_path)
    return env


def build_command(args: argparse.Namespace, case: ExperimentCase) -> list[str]:
    short_rate = 1.0 - args.long_rate
    command = [
        args.go,
        "run",
        "./experiment",
        "synthetic",
        "-baseline",
        ",".join(args.methods),
        "-t",
        str(args.threads),
        "-b",
        str(args.blocks),
        "-bt",
        str(args.tx_per_block),
        "-sk",
        case.skew,
        "-ar",
        str(args.address_rate),
        "-lr",
        format_float(args.long_rate),
        "-sr",
        format_float(short_rate),
        "-wa",
        format_float(args.watermark_alpha),
        "-wb",
        format_float(args.watermark_beta),
        "-f",
        str(case.fibonacci_n),
        "-ta",
    ]
    if case.short_fibonacci_loop is not None:
        command.extend(["-sfln", str(case.short_fibonacci_loop)])
    if case.long_fibonacci_loop is not None:
        command.extend(["-lfln", str(case.long_fibonacci_loop)])
    if args.recursive:
        command.append("-r")
    return command


def command_text(command: list[str]) -> str:
    return " ".join(command)


def parse_tps_summary(output: str) -> dict[str, tuple[str, str]]:
    parsed: dict[str, tuple[str, str]] = {}
    in_summary = False
    for line in output.splitlines():
        if line.startswith("========== Baseline TPS Summary"):
            in_summary = True
            continue
        if in_summary and line.startswith("=========="):
            break
        if not in_summary:
            continue
        match = SUMMARY_ROW_RE.match(line)
        if not match:
            continue
        method = match.group("method")
        tps = "" if match.group("tps") == "N/A" else match.group("tps")
        latency = "" if match.group("latency") == "N/A" else match.group("latency")
        parsed[method] = (tps, latency)
    return parsed


def parse_abort_metrics(output: str, methods: tuple[str, ...]) -> dict[str, tuple[str, str]]:
    metrics: dict[str, tuple[str, str]] = {}
    methods_set = set(methods)
    current_method = ""
    pending_count = ""
    saw_baseline_marker = False

    for line in output.splitlines():
        baseline_match = BASELINE_RE.match(line)
        if baseline_match:
            current_method = baseline_match.group("method")
            saw_baseline_marker = True
            pending_count = ""
            continue

        count_match = ABORT_COUNT_RE.search(line)
        if count_match:
            pending_count = count_match.group("count")
            continue

        cost_match = ABORT_COST_RE.search(line)
        if not cost_match:
            continue

        if current_method in methods_set:
            metrics[current_method] = (pending_count, cost_match.group("cost"))
        elif not saw_baseline_marker:
            method_index = len(metrics)
            if method_index < len(methods):
                metrics[methods[method_index]] = (pending_count, cost_match.group("cost"))
        pending_count = ""
    return metrics


def base_row(
    args: argparse.Namespace,
    case: ExperimentCase,
    method: str,
    trial: int,
    log_file: Path,
    command: list[str],
) -> dict[str, object]:
    short_rate = 1.0 - args.long_rate
    return {
        "experiment": case.experiment,
        "case_id": case.case_id,
        "trial": trial,
        "method": method,
        "long_rate": format_float(args.long_rate),
        "short_rate": format_float(short_rate),
        "skew": case.skew,
        "fibonacci_n": case.fibonacci_n,
        "short_fibonacci_loop": "" if case.short_fibonacci_loop is None else case.short_fibonacci_loop,
        "long_fibonacci_loop": "" if case.long_fibonacci_loop is None else case.long_fibonacci_loop,
        "abort_count": "",
        "abort_cost": "",
        "tps": "",
        "latency_seconds": "",
        "status": "pending",
        "return_code": "",
        "duration_seconds": "",
        "log_file": str(log_file),
        "command": command_text(command),
    }


def run_case(
    args: argparse.Namespace,
    case: ExperimentCase,
    trial: int,
    log_dir: Path,
) -> list[dict[str, object]]:
    command = build_command(args, case)
    log_file = log_dir / f"{case.experiment}_{case.case_id}_trial{trial:02d}.log"
    rows = [base_row(args, case, method, trial, log_file, command) for method in args.methods]

    if args.dry_run:
        print(command_text(command), flush=True)
        return [{**row, "status": "dry-run", "return_code": 0} for row in rows]

    start = time.time()
    try:
        completed = subprocess.run(
            command,
            cwd=repo_root(),
            env=build_env(args, case),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=args.timeout if args.timeout > 0 else None,
        )
        output = completed.stdout
        return_code: int | str = completed.returncode
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout or ""
        return_code = "timeout"
        timed_out = True
    duration = time.time() - start
    log_file.write_text(output, encoding="utf-8", errors="replace")

    tps_summary = parse_tps_summary(output)
    abort_metrics = parse_abort_metrics(output, args.methods)
    result_rows: list[dict[str, object]] = []
    for row in rows:
        method = str(row["method"])
        tps, latency = tps_summary.get(method, ("", ""))
        abort_count, abort_cost = abort_metrics.get(method, ("", ""))
        status = "ok"
        if timed_out:
            status = "timeout"
        elif return_code != 0:
            status = "failed"
        elif not abort_cost:
            status = "abort_cost_not_found"
        elif not tps:
            status = "tps_not_found"
        result_rows.append(
            {
                **row,
                "abort_count": abort_count,
                "abort_cost": abort_cost,
                "tps": tps,
                "latency_seconds": latency,
                "status": status,
                "return_code": return_code,
                "duration_seconds": f"{duration:.3f}",
            }
        )
    return result_rows


def write_rows(path: Path, rows: list[dict[str, object]], append: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a" if append else "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        if not append:
            writer.writeheader()
        writer.writerows(rows)
        csv_file.flush()
        os.fsync(csv_file.fileno())


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run VLDB revision abort-cost experiments.")
    parser.add_argument("--output-dir", default=str(Path("experiment") / "results" / "revision_abort_cost"))
    parser.add_argument("--log-dir", default="", help="Default: <output-dir>/logs.")
    parser.add_argument(
        "--monitor-dir",
        default="",
        help='JANUS_MONITOR_BASE_PATH root. Default: <output-dir>/monitor. Use "default" to keep project default.',
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--go", default="go")
    parser.add_argument("--go-cache", default=str(Path(tempfile.gettempdir()) / "miex-go-build-cache"))
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--blocks", type=int, default=10)
    parser.add_argument("--tx-per-block", type=int, default=2000)
    parser.add_argument("--address-rate", type=int, default=4)
    parser.add_argument("--watermark-alpha", type=float, default=1.5)
    parser.add_argument("--watermark-beta", type=float, default=3.5)
    parser.add_argument("--long-rate", type=parse_rate, default=parse_rate("20%"))
    parser.add_argument("--skews", type=parse_skew_list, default=parse_skew_list(DEFAULT_SKEWS))
    parser.add_argument("--long-fibonacci-loops", type=parse_int_list, default=parse_int_list(DEFAULT_LONG_LOOPS))
    parser.add_argument("--loop-sweep-short-fibonacci-loop", type=int, default=10)
    parser.add_argument("--trials", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=0, help="Per-case timeout in seconds; 0 disables timeout.")
    parser.add_argument("--recursive", action="store_true")
    parser.add_argument(
        "--methods",
        default=",".join(DEFAULT_METHODS),
        help="Comma-separated methods. Janus must appear before abort-cost baselines that reuse its cost trace.",
    )
    args = parser.parse_args(argv)

    args.methods = tuple(parse_csv_list(args.methods))
    if not args.methods:
        raise SystemExit("--methods must not be empty")
    if "janus" not in args.methods:
        raise SystemExit("--methods must include janus so abort-cost baselines can reuse its cost trace")
    if args.methods[0] != "janus":
        raise SystemExit("--methods must start with janus")
    if args.threads <= 0 or args.blocks <= 0 or args.tx_per_block <= 0 or args.address_rate <= 0:
        raise SystemExit("threads, blocks, tx-per-block, and address-rate must be greater than 0")
    if args.trials <= 0:
        raise SystemExit("--trials must be greater than 0")
    if args.loop_sweep_short_fibonacci_loop <= 0:
        raise SystemExit("--loop-sweep-short-fibonacci-loop must be greater than 0")

    args.output_dir = str(resolve_path(args.output_dir))
    if not args.log_dir:
        args.log_dir = str(Path(args.output_dir) / "logs")
    if not args.monitor_dir:
        args.monitor_dir = str(Path(args.output_dir) / "monitor")
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output_dir)
    log_dir = resolve_path(args.log_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "summary.csv"
    if summary_path.exists() and args.overwrite:
        summary_path.unlink()
    if summary_path.exists() and not args.overwrite:
        raise SystemExit(f"output already exists: {summary_path}; use --overwrite to replace it")

    cases = build_cases(args)
    print(f"repo: {repo_root()}")
    print(f"output: {summary_path}")
    print(f"logs: {log_dir}")
    print(f"methods: {','.join(args.methods)}")
    print(f"cases: {len(cases)}")
    print(f"started at: {dt.datetime.now().isoformat(timespec='seconds')}")

    failures = 0
    append = False
    for case_index, case in enumerate(cases, start=1):
        for trial in range(1, args.trials + 1):
            print(f"[{case_index}/{len(cases)}] {case.experiment} {case.case_id} trial={trial}", flush=True)
            rows = run_case(args, case, trial, log_dir)
            failures += sum(1 for row in rows if row["status"] not in {"ok", "dry-run"})
            write_rows(summary_path, rows, append=append)
            append = True
            ok_count = sum(1 for row in rows if row["status"] in {"ok", "dry-run"})
            print(f"  rows={len(rows)} ok={ok_count}", flush=True)

    print(f"finished at: {dt.datetime.now().isoformat(timespec='seconds')}")
    if failures:
        print(f"completed with {failures} failed rows; inspect logs under {log_dir}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
