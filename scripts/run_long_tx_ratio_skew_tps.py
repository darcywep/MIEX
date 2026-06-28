#!/usr/bin/env python3
"""Run synthetic TPS experiments for long-transaction ratio and workload skew.

Default experiment matrix:
  - long transaction rates: 30%, 15%, 10%, 15%
  - workload skew values: 0.4, 0.6, 0.8, 1.0, 1.2
  - methods: all synthetic baselines exposed by `go run ./experiment synthetic`

The script writes one CSV per parameter combination. Each CSV contains one row
per method with TPS, latency, status, log path, and the exact command used.
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
from dataclasses import dataclass
from pathlib import Path


DEFAULT_LONG_RATES = "30%,15%,10%,15%"
DEFAULT_SKEWS = "0.4,0.6,0.8,1.0,1.2"
DEFAULT_METHODS = (
    "harmony",
    "schain",
    "serial",
    "optme",
    "optme_paper",
    "aria",
    "janus",
    "Non_Early_Next_Batch_janus",
    "Non_Maximum_Commit_Validation",
    "newHarmony",
    "mvschedo",
    "quecc",
    "pilotfish",
    "thunderbolt",
)
VALID_METHODS = DEFAULT_METHODS + (
    "janus_cost_only",
    "janus_lp_relaxation",
)

CSV_COLUMNS = (
    "method",
    "long_rate",
    "short_rate",
    "skew",
    "trial",
    "long_txs_per_block",
    "short_txs_per_block",
    "actual_txs_per_block",
    "tps",
    "latency_seconds",
    "tx_type_misclassification_rate",
    "tx_type_misclassification_seed",
    "status",
    "return_code",
    "duration_seconds",
    "log_file",
    "command",
)

NUMBER_RE = r"([0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)"
SUMMARY_ROW_RE = re.compile(
    rf"^\s*(?P<method>[A-Za-z0-9_]+)\s+(?P<tps>{NUMBER_RE}|N/A)\s+(?P<latency>{NUMBER_RE}|N/A)\s*$"
)


@dataclass(frozen=True)
class ParamCase:
    index: int
    long_rate: float
    skew: str


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_rate_token(value: str) -> float:
    token = value.strip()
    if token.endswith("%"):
        token = token[:-1].strip()
        scale = 100.0
    else:
        scale = 1.0
    try:
        rate = float(token) / scale
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid rate: {value!r}") from exc
    if rate < 0 or rate > 1:
        raise argparse.ArgumentTypeError(f"rate must be in [0, 1]: {value!r}")
    return rate


def parse_rate_list(value: str) -> list[float]:
    rates = [parse_rate_token(item) for item in parse_csv_list(value)]
    if not rates:
        raise argparse.ArgumentTypeError("rate list must not be empty")
    return rates


def parse_skew_list(value: str) -> list[str]:
    skews = parse_csv_list(value)
    if not skews:
        raise argparse.ArgumentTypeError("skew list must not be empty")
    for skew in skews:
        try:
            float(skew)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid skew: {skew!r}") from exc
    return skews


def parse_methods(value: str) -> tuple[list[str], bool]:
    if value.strip().lower() == "all":
        return list(DEFAULT_METHODS), True
    methods = parse_csv_list(value)
    if not methods:
        raise argparse.ArgumentTypeError("method list must not be empty")
    unknown = [method for method in methods if method not in VALID_METHODS]
    if unknown:
        raise argparse.ArgumentTypeError(
            "unknown method(s): " + ", ".join(unknown) + "; valid methods: all," + ",".join(VALID_METHODS)
        )
    return methods, False


def parse_probability_arg(value: str) -> str:
    return format_float(parse_rate_token(value))


def format_float(value: float) -> str:
    return f"{value:.10g}"


def sanitize_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")


def format_rate_for_filename(rate: float) -> str:
    percent = rate * 100
    if percent.is_integer():
        return f"{int(percent)}pct"
    return sanitize_filename(f"{percent:.4g}".replace(".", "p") + "pct")


def format_skew_for_filename(skew: str) -> str:
    return "skew" + sanitize_filename(skew.replace(".", "p"))


def case_stem(case: ParamCase) -> str:
    return f"case{case.index:03d}_long{format_rate_for_filename(case.long_rate)}_{format_skew_for_filename(case.skew)}"


def planned_cases(long_rates: list[float], skews: list[str]) -> list[ParamCase]:
    cases: list[ParamCase] = []
    index = 1
    for long_rate in long_rates:
        for skew in skews:
            cases.append(ParamCase(index=index, long_rate=long_rate, skew=skew))
            index += 1
    return cases


def resolve_path(path: str | Path) -> Path:
    output = Path(path).expanduser()
    if not output.is_absolute():
        output = repo_root() / output
    return output


def build_env(args: argparse.Namespace, case: ParamCase) -> dict[str, str]:
    env = os.environ.copy()
    if args.go_cache.lower() != "default":
        go_cache = resolve_path(args.go_cache)
        go_cache.mkdir(parents=True, exist_ok=True)
        env["GOCACHE"] = str(go_cache)
    if args.monitor_dir.lower() != "default":
        monitor_root = resolve_path(args.monitor_dir)
        monitor_path = monitor_root / case_stem(case)
        monitor_path.mkdir(parents=True, exist_ok=True)
        env["JANUS_MONITOR_BASE_PATH"] = str(monitor_path)
    return env


def build_command(args: argparse.Namespace, case: ParamCase, baseline: str) -> list[str]:
    short_rate = 1.0 - case.long_rate
    command = [
        args.go,
        "run",
        "./experiment",
        "synthetic",
        "-baseline",
        baseline,
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
        format_float(case.long_rate),
        "-sr",
        format_float(short_rate),
        "-wa",
        format_float(args.watermark_alpha),
        "-wb",
        format_float(args.watermark_beta),
        "-f",
        str(args.fibonacci_n),
        "-sfln",
        str(args.short_fibonacci_loop),
        "-lfln",
        str(args.long_fibonacci_loop),
        "-tmr",
        args.tx_type_misclassification_rate,
        "-tms",
        str(args.tx_type_misclassification_seed),
    ]
    if args.recursive:
        command.append("-r")
    return command


def command_text(command: list[str]) -> str:
    return " ".join(command)


def parse_tps_summary(output: str) -> dict[str, tuple[str, str]]:
    in_summary = False
    parsed: dict[str, tuple[str, str]] = {}
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


def load_completed_rows(output_path: Path) -> set[tuple[str, str]]:
    if not output_path.exists():
        return set()

    completed: set[tuple[str, str]] = set()
    with output_path.open("r", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            if row.get("status") in {"ok", "dry-run"} and row.get("method"):
                completed.add((row["method"], row.get("trial", "1")))
    return completed


def base_row(
    args: argparse.Namespace,
    case: ParamCase,
    method: str,
    trial: int,
    log_file: Path,
    command: list[str],
) -> dict[str, object]:
    long_count = int(args.tx_per_block * case.long_rate)
    short_rate = 1.0 - case.long_rate
    short_count = int(args.tx_per_block * short_rate)
    return {
        "method": method,
        "long_rate": format_float(case.long_rate),
        "short_rate": format_float(short_rate),
        "skew": case.skew,
        "trial": trial,
        "long_txs_per_block": long_count,
        "short_txs_per_block": short_count,
        "actual_txs_per_block": long_count + short_count,
        "tps": "",
        "latency_seconds": "",
        "tx_type_misclassification_rate": args.tx_type_misclassification_rate,
        "tx_type_misclassification_seed": args.tx_type_misclassification_seed,
        "status": "pending",
        "return_code": "",
        "duration_seconds": "",
        "log_file": str(log_file),
        "command": command_text(command),
    }


def run_baseline_all(
    args: argparse.Namespace,
    case: ParamCase,
    trial: int,
    methods: list[str],
    log_dir: Path,
) -> tuple[list[dict[str, object]], int]:
    command = build_command(args, case, "all")
    log_file = log_dir / f"{case_stem(case)}_trial{trial:02d}_all.log"
    rows = [base_row(args, case, method, trial, log_file, command) for method in methods]

    if args.dry_run:
        print(command_text(command))
        return [{**row, "status": "dry-run", "return_code": 0} for row in rows], 0

    start = time.time()
    completed = subprocess.run(
        command,
        cwd=repo_root(),
        env=build_env(args, case),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    duration = time.time() - start
    output = completed.stdout
    log_file.write_text(output, encoding="utf-8", errors="replace")

    parsed = parse_tps_summary(output)
    result_rows: list[dict[str, object]] = []
    failures = 0
    for row in rows:
        method = str(row["method"])
        tps, latency = parsed.get(method, ("", ""))
        status = "ok" if completed.returncode == 0 and tps else "tps_not_found"
        if completed.returncode != 0:
            status = "failed"
        if status != "ok":
            failures += 1
        result_rows.append(
            {
                **row,
                "tps": tps,
                "latency_seconds": latency,
                "status": status,
                "return_code": completed.returncode,
                "duration_seconds": f"{duration:.3f}",
            }
        )
    return result_rows, failures


def run_one_method(
    args: argparse.Namespace,
    case: ParamCase,
    trial: int,
    method: str,
    log_dir: Path,
) -> tuple[dict[str, object], int]:
    command = build_command(args, case, method)
    log_file = log_dir / f"{case_stem(case)}_trial{trial:02d}_{sanitize_filename(method)}.log"
    row = base_row(args, case, method, trial, log_file, command)

    if args.dry_run:
        print(command_text(command))
        return {**row, "status": "dry-run", "return_code": 0}, 0

    start = time.time()
    completed = subprocess.run(
        command,
        cwd=repo_root(),
        env=build_env(args, case),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    duration = time.time() - start
    output = completed.stdout
    log_file.write_text(output, encoding="utf-8", errors="replace")

    parsed = parse_tps_summary(output)
    tps, latency = parsed.get(method, ("", ""))
    status = "ok" if completed.returncode == 0 and tps else "tps_not_found"
    if completed.returncode != 0:
        status = "failed"
    return (
        {
            **row,
            "tps": tps,
            "latency_seconds": latency,
            "status": status,
            "return_code": completed.returncode,
            "duration_seconds": f"{duration:.3f}",
        },
        0 if status == "ok" else 1,
    )


def write_rows(output_path: Path, rows: list[dict[str, object]], append: bool) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a" if append else "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        if not append:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
        csv_file.flush()
        os.fsync(csv_file.fileno())


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run all methods under long-transaction-rate and workload-skew synthetic loads."
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path("experiment") / "results" / "long_tx_ratio_skew"),
        help="Directory where one CSV per parameter combination is written.",
    )
    parser.add_argument(
        "--log-dir",
        default="",
        help="Directory for per-run logs. Default: <output-dir>/logs.",
    )
    parser.add_argument(
        "--monitor-dir",
        default="",
        help='JANUS_MONITOR_BASE_PATH root for Go outputs. Default: <output-dir>/monitor. Use "default" to keep project default.',
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing combination CSV files.")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Append to existing CSV files and skip completed method/trial rows.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Write dry-run rows and print commands without running Go.")
    parser.add_argument("--go", default="go", help="Go executable to use.")
    parser.add_argument(
        "--go-cache",
        default=str(Path(tempfile.gettempdir()) / "miex-go-build-cache"),
        help='GOCACHE for Go subprocesses. Use "default" to keep Go default.',
    )
    parser.add_argument("--threads", type=int, default=16, help="Value for synthetic -t.")
    parser.add_argument("--blocks", type=int, default=10, help="Value for synthetic -b.")
    parser.add_argument("--tx-per-block", type=int, default=2000, help="Value for synthetic -bt.")
    parser.add_argument("--address-rate", type=int, default=4, help="Value for synthetic -ar.")
    parser.add_argument("--watermark-alpha", type=float, default=1.5, help="Value for synthetic -wa.")
    parser.add_argument("--watermark-beta", type=float, default=3.5, help="Value for synthetic -wb.")
    parser.add_argument("--fibonacci-n", type=int, default=10, help="Value for synthetic -f.")
    parser.add_argument("--short-fibonacci-loop", type=int, default=20, help="Value for synthetic -sfln.")
    parser.add_argument("--long-fibonacci-loop", type=int, default=40, help="Value for synthetic -lfln.")
    parser.add_argument("--recursive", action="store_true", help="Pass synthetic -r.")
    parser.add_argument(
        "--tx-type-misclassification-rate",
        default="0",
        type=parse_probability_arg,
        help="Value for synthetic -tmr.",
    )
    parser.add_argument("--tx-type-misclassification-seed", type=int, default=1, help="Value for synthetic -tms.")
    parser.add_argument("--trials", type=int, default=1, help="Repeated runs per parameter combination.")
    parser.add_argument(
        "--long-rates",
        default=DEFAULT_LONG_RATES,
        help='Comma-separated long transaction rates. Accepts decimals or percentages, e.g. "0.3,15%%".',
    )
    parser.add_argument(
        "--skews",
        default=DEFAULT_SKEWS,
        help='Comma-separated workload skew values for synthetic -sk, e.g. "0.4,0.6".',
    )
    parser.add_argument(
        "--methods",
        default="all",
        help="Comma-separated method list, or all. Default: all.",
    )
    args = parser.parse_args(argv)

    args.long_rates = parse_rate_list(args.long_rates)
    args.skews = parse_skew_list(args.skews)
    args.methods, args.run_all_in_one_process = parse_methods(args.methods)
    args.output_dir = str(resolve_path(args.output_dir))
    if not args.log_dir:
        args.log_dir = str(Path(args.output_dir) / "logs")
    if not args.monitor_dir:
        args.monitor_dir = str(Path(args.output_dir) / "monitor")

    if args.overwrite and args.resume:
        raise SystemExit("--overwrite and --resume cannot be used together")
    if args.threads <= 0:
        raise SystemExit("--threads must be greater than 0")
    if args.blocks <= 0:
        raise SystemExit("--blocks must be greater than 0")
    if args.tx_per_block <= 0:
        raise SystemExit("--tx-per-block must be greater than 0")
    if args.address_rate <= 0:
        raise SystemExit("--address-rate must be greater than 0")
    if args.trials <= 0:
        raise SystemExit("--trials must be greater than 0")

    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output_dir)
    log_dir = resolve_path(args.log_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    cases = planned_cases(args.long_rates, args.skews)
    print(f"repo: {repo_root()}")
    print(f"output dir: {output_dir}")
    print(f"logs: {log_dir}")
    print(f"methods: {','.join(args.methods)}")
    print(f"parameter CSV files: {len(cases)}")
    print(f"planned command groups: {len(cases) * args.trials if args.run_all_in_one_process else len(cases) * args.trials * len(args.methods)}")
    print(f"started at: {dt.datetime.now().isoformat(timespec='seconds')}")

    failures = 0
    for case in cases:
        output_path = output_dir / f"{case_stem(case)}.csv"
        if output_path.exists() and args.overwrite:
            output_path.unlink()
        if output_path.exists() and not args.resume:
            raise SystemExit(
                f"output already exists: {output_path}\n"
                "Use --overwrite to replace it, or --resume to skip completed rows."
            )

        completed = load_completed_rows(output_path) if args.resume else set()
        append = output_path.exists() and args.resume
        print(f"[{case.index}/{len(cases)}] long_rate={format_float(case.long_rate)} skew={case.skew} -> {output_path.name}")

        rows_to_write: list[dict[str, object]] = []
        for trial in range(1, args.trials + 1):
            missing_methods = [method for method in args.methods if (method, str(trial)) not in completed]
            if not missing_methods:
                print(f"  trial={trial} skip completed")
                continue

            if args.run_all_in_one_process:
                print(f"  trial={trial} baseline=all methods={len(missing_methods)}")
                rows, run_failures = run_baseline_all(args, case, trial, args.methods, log_dir)
                rows = [row for row in rows if (str(row["method"]), str(row["trial"])) not in completed]
                rows_to_write.extend(rows)
                failures += sum(1 for row in rows if row["status"] not in {"ok", "dry-run"})
                if run_failures:
                    print(f"    failures={run_failures}")
                else:
                    ok_count = sum(1 for row in rows if row["status"] in {"ok", "dry-run"})
                    print(f"    rows={ok_count}")
            else:
                for method in missing_methods:
                    print(f"  trial={trial} method={method}")
                    row, run_failures = run_one_method(args, case, trial, method, log_dir)
                    rows_to_write.append(row)
                    failures += run_failures
                    print(f"    status={row['status']} tps={row['tps']} duration={row['duration_seconds']}s")

        if rows_to_write:
            write_rows(output_path, rows_to_write, append=append)

    print(f"finished at: {dt.datetime.now().isoformat(timespec='seconds')}")
    if failures:
        print(f"completed with {failures} failed rows; inspect logs under {log_dir}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
