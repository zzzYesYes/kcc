"""Summarize Hybrid pipeline A/B results and inference-window occupancy."""

from __future__ import annotations

import argparse
import json
import os
import statistics

import boto3
from botocore.config import Config


def client():
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"], region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"), config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))


def occupancy(events):
    starts = {}
    intervals = []
    queue_waits = []
    max_active = 0
    max_ready = 0
    for event in events:
        max_active = max(max_active, int(event.get("active_slots", 0)))
        max_ready = max(max_ready, int(event.get("ready_queue", 0)))
        identity = (event.get("slot"), event.get("document_id"), event.get("page_start"))
        if event["event"] == "inference_start":
            starts[identity] = event["timestamp"]
            queue_waits.append(float(event.get("queue_wait_seconds", 0)))
        elif event["event"] == "inference_end" and identity in starts:
            intervals.append((starts.pop(identity), event["timestamp"]))
    points = sorted([(start, 1) for start, _ in intervals] + [(end, -1) for _, end in intervals], key=lambda item: (item[0], item[1]))
    active = 0
    previous = None
    covered = overlap = 0.0
    for timestamp, delta in points:
        if previous is not None:
            duration = timestamp - previous
            if active > 0:
                covered += duration
            if active > 1:
                overlap += duration
        active += delta
        previous = timestamp
    span = points[-1][0] - points[0][0] if points else 0
    return {
        "window_count": len(intervals), "inference_span_seconds": round(span, 3),
        "inference_covered_seconds": round(covered, 3),
        "inference_coverage_percent": round(100 * covered / span, 2) if span else None,
        "dual_slot_overlap_seconds": round(overlap, 3),
        "dual_slot_overlap_percent": round(100 * overlap / span, 2) if span else None,
        "queue_wait_avg_seconds": round(statistics.mean(queue_waits), 4) if queue_waits else None,
        "queue_wait_max_seconds": round(max(queue_waits), 4) if queue_waits else None,
        "max_active_slots": max_active, "max_ready_queue": max_ready,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    args = parser.parse_args()
    s3 = client()
    summary = json.loads(s3.get_object(Bucket=args.bucket, Key=f"{args.prefix}/_SUMMARY.json")["Body"].read())
    services = {}
    for result in summary["results"]:
        if result["service"] in services:
            continue
        key = f"{result['output_prefix']}/artifacts/window-profile.jsonl"
        text = s3.get_object(Bucket=args.bucket, Key=key)["Body"].read().decode()
        services[result["service"]] = occupancy([json.loads(line) for line in text.splitlines() if line.strip()])
    output = {
        "summary": {key: summary[key] for key in ("pdf_count", "success_count", "image_analysis_verified_count", "page_count", "elapsed_seconds", "pages_per_second")},
        "documents": [{"document_id": row["document_id"], "service": row["service"], "pages": row["page_count"], "details": row["image_analysis"]["details_block_count"], "images": row["image_count"], "elapsed_seconds": row["elapsed_seconds"]} for row in summary["results"]],
        "services": services,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
