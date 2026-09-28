from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from .atomic_writer import atomic_write_json
from .minio_client import ObjectStore


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProgressTracker:
    def __init__(
        self,
        store: ObjectStore,
        bucket: str,
        output_prefix: str,
        total_documents: int,
        stage: str,
    ):
        self.store = store
        self.bucket = bucket
        self.output_prefix = output_prefix.rstrip("/")
        self.lock = threading.Lock()
        self.state: dict[str, Any] = {
            "stage": stage,
            "total_documents": total_documents,
            "completed_documents": 0,
            "failed_documents": 0,
            "pending_documents": total_documents,
            "active_documents": [],
            "skipped_documents": 0,
            "updated_at": utc_now(),
        }
        if stage == "stage2":
            self.state.update(
                {
                    "eligible_blocks": 0,
                    "qa_candidates": 0,
                    "qa_verified": 0,
                    "mcq_candidates": 0,
                    "mcq_verified": 0,
                    "rejected": 0,
                    "qwen_requests": 0,
                    "qwen_retries": 0,
                }
            )
        self.flush()

    def flush(self) -> None:
        self.state["updated_at"] = utc_now()
        atomic_write_json(
            self.store,
            self.bucket,
            f"{self.output_prefix}/_PROGRESS.json",
            self.state,
        )

    def start(self, document_id: str) -> None:
        with self.lock:
            self.state["active_documents"].append(document_id)
            self.state["pending_documents"] -= 1
            self.flush()

    def finish(self, document_id: str, status: str, metrics: dict[str, int]) -> None:
        with self.lock:
            if document_id in self.state["active_documents"]:
                self.state["active_documents"].remove(document_id)
            if status == "success":
                self.state["completed_documents"] += 1
            elif status == "skipped":
                self.state["completed_documents"] += 1
                self.state["skipped_documents"] += 1
            else:
                self.state["failed_documents"] += 1
            for key, value in metrics.items():
                self.state[key] = int(self.state.get(key, 0)) + int(value)
            self.flush()
