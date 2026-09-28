"""Summarize JSONL monitor files emitted by the dual MinerU Ray actors."""

from __future__ import annotations

import argparse
import glob
import json
import statistics
from pathlib import Path
from typing import Any, Callable


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * fraction)]


def values(rows: list[dict[str, Any]], getter: Callable[[dict[str, Any]], float | None]) -> list[float]:
    return [value for row in rows if (value := getter(row)) is not None]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    args = parser.parse_args()

    report: dict[str, Any] = {}
    pattern = str(args.run_dir / "service-*" / "service-monitor.jsonl")
    for raw_path in sorted(glob.glob(pattern)):
        path = Path(raw_path)
        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        if not rows:
            continue
        service = rows[0]["service"]
        aicore = values(rows, lambda row: row["npu"].get("aicore_percent"))
        hbm = values(rows, lambda row: row["npu"].get("hbm_percent"))
        running = values(rows, lambda row: row["vllm"].get("running_requests"))
        waiting = values(rows, lambda row: row["vllm"].get("waiting_requests"))
        actor_rss = values(rows, lambda row: row.get("actor_rss_bytes"))
        report[service] = {
            "samples": len(rows),
            "aicore_percent": {
                "avg": round(statistics.fmean(aicore), 2),
                "p50": percentile(aicore, 0.50),
                "p90": percentile(aicore, 0.90),
                "max": max(aicore),
                "nonzero_share": round(sum(value > 0 for value in aicore) / len(aicore), 3),
            },
            "hbm_percent": {"avg": round(statistics.fmean(hbm), 2), "max": max(hbm)},
            "vllm_running": {
                "avg": round(statistics.fmean(running), 2),
                "p90": percentile(running, 0.90),
                "max": max(running),
            },
            "vllm_waiting": {
                "avg": round(statistics.fmean(waiting), 2),
                "p90": percentile(waiting, 0.90),
                "max": max(waiting),
                "positive_share": round(sum(value > 0 for value in waiting) / len(waiting), 3),
            },
            "actor_rss_peak_gib": round(max(actor_rss) / 2**30, 2),
        }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
