"""Print a small terminal-friendly snapshot for a Hybrid MinerU Ray job."""

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
            f"finished status={summary['status']} success={summary['success_count']}/{summary['pdf_count']} "
            f"details={summary['image_analysis_verified_count']} pages={summary['page_count']} "
            f"elapsed={summary['elapsed_seconds']}s pages/s={summary['pages_per_second']}"
        )
        return
    progress_path = args.run_dir / "progress.json"
    if not progress_path.exists():
        print("starting: coordinator has not emitted status yet")
        return
    state = json.loads(progress_path.read_text())
    active = ", ".join(f"{item['service']}:{item['document_id']}" for item in state["active"]) or "none"
    print(f"running total={state['total']} skipped={state['skipped']} completed={state['completed']} pending={state['pending']} active={active}")


if __name__ == "__main__":
    main()
