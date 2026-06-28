#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


EXPECTED_METHODS = [
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
    "blockstm",
]

NUMBER = r"(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
SUMMARY_ROW_RE = re.compile(
    rf"^\s*(?P<method>[A-Za-z_][A-Za-z0-9_-]*)\s+"
    rf"(?P<tps>{NUMBER}|N/A)\s+"
    rf"(?P<latency>{NUMBER}|N/A)\s*$"
)
BLOCKSTM_TPS_RES = [
    re.compile(rf"\bBlock[-_ ]?STM\s+TPS\s*[:=]\s*(?P<tps>{NUMBER})", re.IGNORECASE),
    re.compile(rf"\bBlock\s+TPS\s*[:=]\s*(?P<tps>{NUMBER})", re.IGNORECASE),
    re.compile(
        rf"^\s*blockstm\s+(?P<tps>{NUMBER})(?:\s+(?P<latency>{NUMBER}))?\s*$",
        re.IGNORECASE | re.MULTILINE,
    ),
]


@dataclass
class MethodResult:
    tps: float | None
    latency_seconds: float | None
    status: str


@dataclass
class CaseResult:
    case_id: str
    long_rate: float
    short_rate: float
    skew: float
    return_code: int | None
    csv_path: Path
    log_path: Path
    rows: list[dict[str, object]]
    missing_methods: list[str]
    timed_out: bool


def parse_float_list(raw: str, *, percent_allowed: bool = False) -> list[float]:
    values: list[float] = []
    for item in raw.split(","):
        token = item.strip()
        if not token:
            continue
        if token.endswith("%"):
            if not percent_allowed:
                raise ValueError(f"percent value is not allowed here: {token}")
            value = float(token[:-1]) / 100.0
        else:
            value = float(token)
            if percent_allowed and value > 1:
                value = value / 100.0
        values.append(value)
    if not values:
        raise ValueError("empty value list")
    return values


def validate_rates(rates: Iterable[float]) -> None:
    for rate in rates:
        if rate < 0 or rate > 1:
            raise ValueError(f"long transaction rate must be in [0, 1], got {rate}")


def parse_tps_output(output: str) -> dict[str, MethodResult]:
    results: dict[str, MethodResult] = {}
    in_summary = False

    for line in output.splitlines():
        stripped = line.strip()
        if "Baseline TPS Summary" in stripped:
            in_summary = True
            continue
        if not in_summary:
            continue
        if not stripped:
            continue
        if set(stripped) == {"="}:
            in_summary = False
            continue
        if stripped.startswith("Baseline") or stripped.startswith("-"):
            continue

        match = SUMMARY_ROW_RE.match(line)
        if not match:
            continue
        tps = none_if_na(match.group("tps"))
        latency = none_if_na(match.group("latency"))
        results[match.group("method")] = MethodResult(tps, latency, "ok")

    if "blockstm" not in results:
        for regex in BLOCKSTM_TPS_RES:
            match = regex.search(output)
            if match:
                results["blockstm"] = MethodResult(
                    float(match.group("tps")),
                    float(match.group("latency")) if match.groupdict().get("latency") else None,
                    "ok",
                )
                break

    return results


def none_if_na(value: str) -> float | None:
    if value == "N/A":
        return None
    return float(value)


def format_float(value: float) -> str:
    return f"{value:.10g}"


def slug_float(value: float) -> str:
    text = f"{value:.2f}".rstrip("0").rstrip(".")
    return text.replace(".", "p")


def pct_label(value: float) -> str:
    return f"{value * 100:.2f}".rstrip("0").rstrip(".") + "%"


def build_command(args: argparse.Namespace, long_rate: float, skew: float) -> list[str]:
    short_rate = 1.0 - long_rate
    cmd = [
        args.go_bin,
        "run",
        ".",
        "synthetic",
        "-baseline",
        "all",
        "-t",
        str(args.threads),
        "-b",
        str(args.blocks),
        "-bt",
        str(args.block_txs),
        "-sk",
        format_float(skew),
        "-ar",
        str(args.address_rate),
        "-lr",
        format_float(long_rate),
        "-sr",
        format_float(short_rate),
        "-wa",
        format_float(args.watermark_alpha),
        "-wb",
        format_float(args.watermark_beta),
        "-f",
        str(args.fibonacci_n),
        "-go-bin",
        args.go_bin,
    ]
    if args.recursive:
        cmd.append("-r=true")
    if args.trace_abort:
        cmd.append("-ta=true")
    if args.janus_path:
        cmd.extend(["-janus-path", str(args.janus_path)])
    if args.blockstm_path:
        cmd.extend(["-blockstm-path", str(args.blockstm_path)])
    return cmd


def run_case(
    args: argparse.Namespace,
    script_dir: Path,
    output_dir: Path,
    index: int,
    total: int,
    long_rate: float,
    skew: float,
) -> CaseResult:
    short_rate = 1.0 - long_rate
    case_id = f"case_{index:03d}_lr{slug_float(long_rate)}_sk{slug_float(skew)}"
    csv_path = output_dir / "csv" / f"{case_id}.csv"
    log_path = output_dir / "logs" / f"{case_id}.log"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = build_command(args, long_rate, skew)
    command_line = shlex.join(cmd)
    print(
        f"[{index}/{total}] long={pct_label(long_rate)} short={pct_label(short_rate)} "
        f"skew={format_float(skew)}",
        flush=True,
    )
    print(f"  $ {command_line}", flush=True)

    if args.dry_run:
        output = ""
        return_code = 0
        timed_out = False
        log_path.write_text(command_line + "\n", encoding="utf-8")
    else:
        try:
            completed = subprocess.run(
                cmd,
                cwd=script_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=args.timeout if args.timeout > 0 else None,
            )
            output = completed.stdout
            return_code = completed.returncode
            timed_out = False
        except subprocess.TimeoutExpired as exc:
            output = exc.stdout or ""
            return_code = None
            timed_out = True

        log_path.write_text("$ " + command_line + "\n\n" + output, encoding="utf-8")

    if args.dry_run:
        parsed = {method: MethodResult(None, None, "dry_run") for method in EXPECTED_METHODS}
    else:
        parsed = parse_tps_output(output)
    rows: list[dict[str, object]] = []
    missing_methods: list[str] = []

    for method in EXPECTED_METHODS:
        result = parsed.get(method)
        if result is None:
            missing_methods.append(method)
            result = MethodResult(None, None, "missing")
        rows.append(
            {
                "case_id": case_id,
                "long_tx_rate": long_rate,
                "short_tx_rate": short_rate,
                "skew": skew,
                "method": method,
                "tps": "" if result.tps is None else result.tps,
                "latency_seconds": "" if result.latency_seconds is None else result.latency_seconds,
                "status": result.status,
                "return_code": "" if return_code is None else return_code,
                "log_path": str(log_path),
            }
        )

    for method, result in sorted(parsed.items()):
        if method in EXPECTED_METHODS:
            continue
        rows.append(
            {
                "case_id": case_id,
                "long_tx_rate": long_rate,
                "short_tx_rate": short_rate,
                "skew": skew,
                "method": method,
                "tps": "" if result.tps is None else result.tps,
                "latency_seconds": "" if result.latency_seconds is None else result.latency_seconds,
                "status": "extra",
                "return_code": "" if return_code is None else return_code,
                "log_path": str(log_path),
            }
        )

    write_csv(csv_path, rows)
    print(f"  -> {csv_path}", flush=True)
    if missing_methods:
        print(f"  missing TPS: {', '.join(missing_methods)}", flush=True)
    if return_code not in (0, None):
        print(f"  command exited with code {return_code}; see {log_path}", flush=True)
    if timed_out:
        print(f"  command timed out; see {log_path}", flush=True)

    return CaseResult(
        case_id=case_id,
        long_rate=long_rate,
        short_rate=short_rate,
        skew=skew,
        return_code=return_code,
        csv_path=csv_path,
        log_path=log_path,
        rows=rows,
        missing_methods=missing_methods,
        timed_out=timed_out,
    )


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = [
        "case_id",
        "long_tx_rate",
        "short_tx_rate",
        "skew",
        "method",
        "tps",
        "latency_seconds",
        "status",
        "return_code",
        "log_path",
    ]
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run synthetic TPS experiments for long-tx-rate/skew combinations."
    )
    parser.add_argument(
        "--long-ratios",
        # default="30%,15%,10%,5%",
        default="5%",
        help="comma-separated long transaction ratios; accepts 0.30 or 30%%",
    )
    parser.add_argument(
        "--skews",
        default="0.4,0.6,0.8,1.0,1.2",
        help="comma-separated Zipf skew values",
    )
    parser.add_argument("-t", "--threads", type=int, default=16)
    parser.add_argument("-b", "--blocks", type=int, default=10)
    parser.add_argument("-bt", "--block-txs", type=int, default=2000)
    parser.add_argument("-ar", "--address-rate", type=int, default=4)
    parser.add_argument("--watermark-alpha", type=float, default=1.5)
    parser.add_argument("--watermark-beta", type=float, default=3.5)
    parser.add_argument("--fibonacci-n", type=int, default=-1)
    parser.add_argument("--recursive", action="store_true")
    parser.add_argument("--trace-abort", action="store_true")
    parser.add_argument("--janus-path", type=Path, default=None)
    parser.add_argument("--blockstm-path", type=Path, default=None)
    parser.add_argument("--go-bin", default="go")
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument(
        "--timeout",
        type=float,
        default=0,
        help="per-case timeout in seconds; 0 disables timeout",
    )
    parser.add_argument(
        "--allow-missing-methods",
        action="store_true",
        help="do not return a failure when a method has no parsed TPS",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print commands and write empty CSVs without running Go experiments",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    long_rates = parse_float_list(args.long_ratios, percent_allowed=True)
    skews = parse_float_list(args.skews)
    validate_rates(long_rates)

    output_dir = args.output_dir
    if output_dir is None:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = script_dir / "results" / f"synthetic_tps_matrix_{stamp}"
    else:
        output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    cases = [(long_rate, skew) for long_rate in long_rates for skew in skews]
    all_rows: list[dict[str, object]] = []
    case_results: list[CaseResult] = []

    for index, (long_rate, skew) in enumerate(cases, start=1):
        case_result = run_case(args, script_dir, output_dir, index, len(cases), long_rate, skew)
        case_results.append(case_result)
        all_rows.extend(case_result.rows)

    summary_path = output_dir / "summary.csv"
    write_csv(summary_path, all_rows)

    failed_commands = [
        case for case in case_results if case.return_code not in (0, None) or case.timed_out
    ]
    cases_with_missing = [case for case in case_results if case.missing_methods]
    print()
    print(f"CSV directory: {output_dir / 'csv'}")
    print(f"Aggregate CSV: {summary_path}")
    print(f"Logs: {output_dir / 'logs'}")
    print(f"Cases: {len(case_results)}")
    print(f"Expected methods per case: {len(EXPECTED_METHODS)}")

    if failed_commands:
        print(f"Cases with command failure or timeout: {len(failed_commands)}")
    if cases_with_missing:
        print(f"Cases with missing TPS values: {len(cases_with_missing)}")

    if failed_commands:
        return 1
    if cases_with_missing and not args.allow_missing_methods:
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
