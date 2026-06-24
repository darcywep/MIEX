#!/usr/bin/env python3
"""Run Janus early-next-batch ablation experiments.

Default experiment matrix:
  - methods: janus, Non_Early_Next_Batch_janus
  - tx type misclassification rates: 0, 0.1, 0.2, 0.3, 1.0
  - short:long ratios: 1:1, 2:1, 4:1, 8:1
  - fixed Go flags: -t 16 -b 10 -bt 2000 -sk 0

The synthetic experiment CLI takes long/short fractions as -lr and -sr.
For short:long = S:L, this script sets:
  -sr = S / (S + L)
  -lr = L / (S + L)
"""

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
from contextlib import ExitStack
from pathlib import Path
from typing import Iterable


DEFAULT_METHODS = ("janus", "Non_Early_Next_Batch_janus")
DEFAULT_MISCLASSIFICATION_RATES = ("0", "0.1", "0.2", "0.3", "1.0")
DEFAULT_SHORT_LONG_RATIOS = ("1:1", "2:1", "4:1", "8:1")

CSV_COLUMNS = (
    "method",
    "misclassification_rate",
    "short_long_ratio",
    "short_rate",
    "long_rate",
    "short_txs_per_block",
    "long_txs_per_block",
    "actual_txs_per_block",
    "tms",
    "trial",
    "tps",
    "status",
    "return_code",
    "duration_seconds",
    "log_file",
    "command",
)

NUMBER_RE = r"([0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_csv_list(value: str) -> list[str]:
    items = [item.strip() for item in value.split(",")]
    return [item for item in items if item]


def parse_ratio(value: str) -> tuple[float, float]:
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)\s*", value)
    if not match:
        raise argparse.ArgumentTypeError(f"invalid short:long ratio: {value!r}")
    short = float(match.group(1))
    long = float(match.group(2))
    if short <= 0 or long <= 0:
        raise argparse.ArgumentTypeError(f"ratio values must be positive: {value!r}")
    total = short + long
    return short / total, long / total


def validate_rate(value: str) -> str:
    try:
        rate = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid misclassification rate: {value!r}") from exc
    if rate < 0 or rate > 1:
        raise argparse.ArgumentTypeError(f"misclassification rate must be in [0, 1]: {value!r}")
    return value


def sanitize_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")


def format_float(value: float) -> str:
    return f"{value:.10g}"


def build_command(args: argparse.Namespace, method: str, tmr: str, sr: float, lr: float) -> list[str]:
    return [
        args.go,
        "run",
        "./experiment",
        "synthetic",
        "-baseline",
        method,
        "-t",
        str(args.threads),
        "-b",
        str(args.blocks),
        "-bt",
        str(args.tx_per_block),
        "-sk",
        str(args.skew),
        "-tmr",
        tmr,
        "-tms",
        str(args.tms),
        "-sr",
        format_float(sr),
        "-lr",
        format_float(lr),
    ]


def build_env(args: argparse.Namespace) -> dict[str, str]:
    env = os.environ.copy()
    if args.go_cache.lower() != "default":
        go_cache = Path(args.go_cache).expanduser()
        if not go_cache.is_absolute():
            go_cache = repo_root() / go_cache
        go_cache.mkdir(parents=True, exist_ok=True)
        env["GOCACHE"] = str(go_cache)
    if args.monitor_dir.lower() != "default":
        monitor_dir = Path(args.monitor_dir).expanduser()
        if not monitor_dir.is_absolute():
            monitor_dir = repo_root() / monitor_dir
        monitor_dir.mkdir(parents=True, exist_ok=True)
        env["JANUS_MONITOR_BASE_PATH"] = str(monitor_dir)
    return env


def parse_tps(output: str, method: str) -> str | None:
    summary_pattern = re.compile(rf"(?m)^\s*{re.escape(method)}\s+{NUMBER_RE}\s+{NUMBER_RE}\s*$")
    summary_match = summary_pattern.search(output)
    if summary_match:
        return summary_match.group(1)

    throughput_pattern = re.compile(rf"TPS \(Throughput\):\s+{NUMBER_RE}")
    throughput_match = throughput_pattern.search(output)
    if throughput_match:
        return throughput_match.group(1)

    return None


def load_completed_keys(output_path: Path) -> set[tuple[str, str, str, str, str]]:
    if not output_path.exists():
        return set()

    completed = set()
    with output_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("status") != "ok" or not row.get("tps"):
                continue
            completed.add(
                (
                    row.get("method", ""),
                    row.get("misclassification_rate", ""),
                    row.get("short_long_ratio", ""),
                    row.get("tms", ""),
                    row.get("trial", ""),
                )
            )
    return completed


def planned_runs(args: argparse.Namespace) -> Iterable[dict[str, object]]:
    for tmr in args.misclassification_rates:
        validate_rate(tmr)
        for ratio in args.short_long_ratios:
            sr, lr = parse_ratio(ratio)
            for method in args.methods:
                for trial in range(1, args.trials + 1):
                    yield {
                        "method": method,
                        "tmr": tmr,
                        "ratio": ratio,
                        "sr": sr,
                        "lr": lr,
                        "trial": trial,
                    }


def resolve_output_base(args: argparse.Namespace) -> Path:
    output_base = Path(args.output).expanduser()
    if not output_base.is_absolute():
        output_base = repo_root() / output_base
    return output_base


def method_output_path(output_base: Path, method: str) -> Path:
    suffix = output_base.suffix or ".csv"
    stem = output_base.stem if output_base.suffix else output_base.name
    return output_base.with_name(f"{stem}_{sanitize_filename(method)}{suffix}")


def prepare_outputs(
    args: argparse.Namespace,
) -> dict[str, tuple[Path, bool, set[tuple[str, str, str, str, str]]]]:
    output_base = resolve_output_base(args)
    output_base.parent.mkdir(parents=True, exist_ok=True)

    outputs: dict[str, tuple[Path, bool, set[tuple[str, str, str, str, str]]]] = {}
    for method in args.methods:
        output_path = method_output_path(output_base, method)
        if output_path.exists() and args.overwrite:
            output_path.unlink()

        if output_path.exists() and not args.resume:
            raise SystemExit(
                f"output already exists: {output_path}\n"
                "Use --overwrite to replace it, or --resume to skip completed rows."
            )

        append = output_path.exists() and args.resume
        completed = load_completed_keys(output_path) if args.resume else set()
        outputs[method] = (output_path, append, completed)
    return outputs


def write_row(writer: csv.DictWriter, row: dict[str, object], csv_file) -> None:
    writer.writerow(row)
    csv_file.flush()
    os.fsync(csv_file.fileno())


def run_one(args: argparse.Namespace, run: dict[str, object], log_dir: Path) -> dict[str, object]:
    method = str(run["method"])
    tmr = str(run["tmr"])
    ratio = str(run["ratio"])
    sr = float(run["sr"])
    lr = float(run["lr"])
    trial = int(run["trial"])

    short_count = int(args.tx_per_block * sr)
    long_count = int(args.tx_per_block * lr)
    actual_count = short_count + long_count

    command = build_command(args, method, tmr, sr, lr)
    command_text = " ".join(command)
    log_name = (
        f"trial{trial:02d}_tmr{sanitize_filename(tmr)}_"
        f"ratio{sanitize_filename(ratio)}_{sanitize_filename(method)}.log"
    )
    log_path = log_dir / log_name

    base_row: dict[str, object] = {
        "method": method,
        "misclassification_rate": tmr,
        "short_long_ratio": ratio,
        "short_rate": format_float(sr),
        "long_rate": format_float(lr),
        "short_txs_per_block": short_count,
        "long_txs_per_block": long_count,
        "actual_txs_per_block": actual_count,
        "tms": args.tms,
        "trial": trial,
        "tps": "",
        "status": "pending",
        "return_code": "",
        "duration_seconds": "",
        "log_file": str(log_path),
        "command": command_text,
    }

    if args.dry_run:
        print(command_text)
        return {**base_row, "status": "dry-run", "return_code": 0}

    start = time.time()
    completed = subprocess.run(
        command,
        cwd=repo_root(),
        env=build_env(args),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    duration = time.time() - start
    output = completed.stdout
    log_path.write_text(output, encoding="utf-8", errors="replace")

    tps = parse_tps(output, method)
    if completed.returncode == 0 and tps is not None:
        status = "ok"
    elif completed.returncode != 0:
        status = "failed"
    else:
        status = "tps_not_found"

    return {
        **base_row,
        "tps": tps or "",
        "status": status,
        "return_code": completed.returncode,
        "duration_seconds": f"{duration:.3f}",
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    default_output = Path("experiment") / "results" / "early_next_batch_ablation.csv"
    parser = argparse.ArgumentParser(
        description="Run Janus vs Non_Early_Next_Batch_janus TPS experiments."
    )
    parser.add_argument(
        "--output",
        default=str(default_output),
        help="CSV base path. The method name is appended so each method gets a separate CSV.",
    )
    parser.add_argument(
        "--log-dir",
        default="",
        help="Directory for per-run logs. Default: <output-stem>_logs next to the CSV.",
    )
    parser.add_argument(
        "--monitor-dir",
        default="",
        help='JANUS_MONITOR_BASE_PATH for Go outputs. Default: <output-stem>_monitor. Use "default" to keep project default.',
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite an existing CSV.")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Append to an existing CSV and skip completed rows with status=ok.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print commands without running them.")
    parser.add_argument("--go", default="go", help="Go executable to use.")
    parser.add_argument(
        "--go-cache",
        default=str(Path(tempfile.gettempdir()) / "miex-go-build-cache"),
        help='GOCACHE for Go subprocesses. Use "default" to keep Go default.',
    )
    parser.add_argument("--threads", type=int, default=16, help="Value for -t.")
    parser.add_argument("--blocks", type=int, default=10, help="Value for -b.")
    parser.add_argument("--tx-per-block", type=int, default=2000, help="Value for -bt.")
    parser.add_argument("--skew", default="0", help="Value for -sk.")
    parser.add_argument("--tms", type=int, default=1, help="Value for -tms.")
    parser.add_argument("--trials", type=int, default=1, help="Number of repeated runs per combination.")
    parser.add_argument(
        "--methods",
        default=",".join(DEFAULT_METHODS),
        help="Comma-separated method list.",
    )
    parser.add_argument(
        "--misclassification-rates",
        default=",".join(DEFAULT_MISCLASSIFICATION_RATES),
        help="Comma-separated -tmr values.",
    )
    parser.add_argument(
        "--short-long-ratios",
        default=",".join(DEFAULT_SHORT_LONG_RATIOS),
        help="Comma-separated short:long ratios.",
    )
    args = parser.parse_args(argv)

    args.methods = parse_csv_list(args.methods)
    args.misclassification_rates = parse_csv_list(args.misclassification_rates)
    args.short_long_ratios = parse_csv_list(args.short_long_ratios)

    if not args.monitor_dir:
        output_path = resolve_output_base(args)
        args.monitor_dir = str(output_path.with_name(output_path.stem + "_monitor"))

    if args.trials <= 0:
        raise SystemExit("--trials must be greater than 0")
    if args.threads <= 0:
        raise SystemExit("--threads must be greater than 0")
    if args.blocks <= 0:
        raise SystemExit("--blocks must be greater than 0")
    if args.tx_per_block <= 0:
        raise SystemExit("--tx-per-block must be greater than 0")
    if not args.methods:
        raise SystemExit("--methods must not be empty")
    if not args.misclassification_rates:
        raise SystemExit("--misclassification-rates must not be empty")
    if not args.short_long_ratios:
        raise SystemExit("--short-long-ratios must not be empty")
    if args.overwrite and args.resume:
        raise SystemExit("--overwrite and --resume cannot be used together")

    for rate in args.misclassification_rates:
        validate_rate(rate)
    for ratio in args.short_long_ratios:
        parse_ratio(ratio)

    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    outputs = prepare_outputs(args)
    output_base = resolve_output_base(args)

    log_root = Path(args.log_dir).expanduser() if args.log_dir else output_base.with_name(output_base.stem + "_logs")
    if not log_root.is_absolute():
        log_root = repo_root() / log_root
    log_root.mkdir(parents=True, exist_ok=True)

    runs = list(planned_runs(args))
    print(f"repo: {repo_root()}")
    print("outputs:")
    for method, (output_path, _, _) in outputs.items():
        print(f"  {method}: {output_path}")
    print(f"logs: {log_root}")
    print(f"planned runs: {len(runs)}")
    print(f"started at: {dt.datetime.now().isoformat(timespec='seconds')}")

    failures = 0
    with ExitStack() as stack:
        writers: dict[str, tuple[csv.DictWriter, object]] = {}
        completed_by_method: dict[str, set[tuple[str, str, str, str, str]]] = {}
        for method, (output_path, append, completed) in outputs.items():
            csv_file = stack.enter_context(output_path.open("a" if append else "w", newline=""))
            writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
            if not append:
                writer.writeheader()
                csv_file.flush()
            writers[method] = (writer, csv_file)
            completed_by_method[method] = completed

        for idx, run in enumerate(runs, start=1):
            method = str(run["method"])
            key = (
                method,
                str(run["tmr"]),
                str(run["ratio"]),
                str(args.tms),
                str(run["trial"]),
            )
            if key in completed_by_method[method]:
                print(f"[{idx}/{len(runs)}] skip completed {key}")
                continue

            tmr = str(run["tmr"])
            ratio = str(run["ratio"])
            trial = int(run["trial"])
            print(f"[{idx}/{len(runs)}] method={method} tmr={tmr} ratio={ratio} trial={trial}")
            method_log_dir = log_root / sanitize_filename(method)
            method_log_dir.mkdir(parents=True, exist_ok=True)
            row = run_one(args, run, method_log_dir)
            writer, csv_file = writers[method]
            write_row(writer, row, csv_file)
            print(f"  status={row['status']} tps={row['tps']} duration={row['duration_seconds']}s")
            if row["status"] not in ("ok", "dry-run"):
                failures += 1

    print(f"finished at: {dt.datetime.now().isoformat(timespec='seconds')}")
    if failures:
        print(f"completed with {failures} failed runs; inspect logs under {log_root}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
