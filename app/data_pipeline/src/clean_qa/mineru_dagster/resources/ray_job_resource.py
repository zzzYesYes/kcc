from __future__ import annotations

import os
import time
from collections.abc import Iterator

from dagster import ConfigurableResource
from ray.job_submission import JobStatus, JobSubmissionClient


class RayJobResource(ConfigurableResource):
    dashboard_address: str = os.environ.get(
        "RAY_DASHBOARD_ADDRESS",
        "http://raycluster-k12-smoke-head-svc.k12.svc.cluster.local:8265",
    )
    working_dir: str = os.environ.get("PIPELINE_WORKING_DIR", "/opt/data-pipeline/src")

    def client(self) -> JobSubmissionClient:
        return JobSubmissionClient(self.dashboard_address)

    def submit(self, job_id: str, entrypoint: str, env_vars: dict[str, str] | None = None) -> str:
        return self.client().submit_job(
            submission_id=job_id,
            entrypoint=entrypoint,
            runtime_env={"working_dir": self.working_dir, "env_vars": env_vars or {}},
        )

    def status(self, job_id: str) -> JobStatus:
        return self.client().get_job_status(job_id)

    def logs(self, job_id: str) -> str:
        return self.client().get_job_logs(job_id)

    def tail_logs(self, job_id: str) -> Iterator[str]:
        client = self.client()
        emitted = 0
        terminal = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.STOPPED}
        while True:
            logs = client.get_job_logs(job_id)
            if len(logs) > emitted:
                chunk = logs[emitted:]
                emitted = len(logs)
                yield from chunk.splitlines()
            if client.get_job_status(job_id) in terminal:
                return
            time.sleep(2.0)

    def wait(self, job_id: str, poll_seconds: float = 2.0, timeout_seconds: float = 7200) -> JobStatus:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            status = self.status(job_id)
            if status in {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.STOPPED}:
                return status
            time.sleep(poll_seconds)
        raise TimeoutError(f"Ray job {job_id} did not finish within {timeout_seconds}s")

    def stop(self, job_id: str) -> bool:
        return self.client().stop_job(job_id)
