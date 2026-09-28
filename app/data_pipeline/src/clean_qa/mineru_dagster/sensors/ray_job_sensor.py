from __future__ import annotations

import json

from dagster import DefaultSensorStatus, RunRequest, SensorEvaluationContext, sensor
from ray.job_submission import JobStatus

from ..jobs.mineru_finalize_job import mineru_finalize_job


@sensor(
    job=mineru_finalize_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"s3", "ray_jobs"},
)
def ray_job_status_sensor(context: SensorEvaluationContext):
    completed = set(json.loads(context.cursor or "[]"))
    s3 = context.resources.s3
    for item in s3.list_prefix("k12-mineru-output", "_control/dagster/submissions/"):
        key = item["Key"]
        state = s3.read_json("k12-mineru-output", key)
        batch_id = state["batch_id"]
        if state.get("submission_mode") != "asynchronous":
            continue
        if batch_id in completed:
            continue
        status = context.resources.ray_jobs.status(state["ray_job_id"])
        if status == JobStatus.SUCCEEDED:
            completed.add(batch_id)
            yield RunRequest(
                run_key=f"finalize-{batch_id}-{state['ray_job_id']}",
                run_config={
                    "ops": {
                        "load_submission_state": {
                            "config": {
                                "batch_id": batch_id,
                                "output_bucket": state["output_bucket"],
                            }
                        }
                    }
                },
            )
        elif status in {JobStatus.FAILED, JobStatus.STOPPED}:
            context.log.error("Ray job %s ended as %s", state["ray_job_id"], status)
            completed.add(batch_id)
    context.update_cursor(json.dumps(sorted(completed)))
