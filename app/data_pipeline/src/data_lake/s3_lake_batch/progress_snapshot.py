"""Print a compact progress snapshot from a dual MinerU Head run directory."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    args = parser.parse_args()

    summary_path = args.run_dir / "summary.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text())
        print(
            "finished "
            f"status={summary['status']} pdf={summary['success_count']}/{summary['pdf_count']} "
            f"pages={summary['page_count']} elapsed={summary['elapsed_seconds']}s "
            f"throughput={summary['pages_per_second']:.3f} pages/s"
        )
        return

    log_path = args.run_dir / "coordinator.jsonl"
    if not log_path.exists() or not log_path.stat().st_size:
        print("starting: coordinator has not emitted a progress sample yet")
        return
    last = json.loads(log_path.read_text().splitlines()[-1])
    parts = [f"completed={last['completed']}", f"pending={last['pending']}"]
    for name, status in sorted(last["statuses"].items()):
        scheduler = status.get("scheduler", {})
        parts.append(
            f"{name}[healthy={status['healthy']} inflight={status['inflight_documents']} "
            f"pages={status['completed_pages']} slots={scheduler.get('active_slots', 0)} "
            f"ready={scheduler.get('ready_queue_depth', 0)}]"
        )
    print(" ".join(parts))


if __name__ == "__main__":
    main()
