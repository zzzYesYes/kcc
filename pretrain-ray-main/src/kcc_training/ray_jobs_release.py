"""Ray Jobs adapter pinned to the release runtime entrypoint."""

from __future__ import annotations

import json
from typing import Mapping, Sequence

from .ray_jobs_rest import RayJobsRest, RayJobsRestError


class ReleaseRayJobsRest(RayJobsRest):
    def submit_once(
        self,
        address: str,
        submission_id: str,
        command: Sequence[str],
        *,
        metadata: Mapping[str, str],
    ) -> str:
        if not command or not all(isinstance(item, str) and item for item in command):
            raise RayJobsRestError(None, "Ray job command is invalid")
        existing = self.detail(address, submission_id)
        if existing is not None:
            existing_metadata = existing.get("metadata", {})
            if not isinstance(existing_metadata, Mapping) or any(
                existing_metadata.get(key) != value for key, value in metadata.items()
            ):
                raise RayJobsRestError(409, "existing Ray job ownership metadata differs")
            return submission_id
        payload = {
            "entrypoint": "python -m kcc_training.runtime.coordinator_release --spec /etc/kcc/run/run.json",
            "submission_id": submission_id,
            "metadata": dict(metadata),
            "runtime_env": {"env_vars": {"KCC_COMMAND_JSON": json.dumps(list(command))}},
        }
        response = self._transport(
            "POST",
            f"{address.rstrip('/')}/api/jobs/",
            json.dumps(payload, separators=(",", ":")).encode(),
        )
        if response.status not in (200, 201):
            after = self.detail(address, submission_id)
            if after is None:
                raise RayJobsRestError(response.status, f"Ray job submit returned {response.status}")
            if not isinstance(after.get("metadata"), Mapping) or any(
                after["metadata"].get(key) != value for key, value in metadata.items()
            ):
                raise RayJobsRestError(409, "Ray job ownership is uncertain")
            return submission_id
        value = self._decode(response, "submit")
        returned = value.get("submission_id", value.get("submissionId", submission_id))
        if returned != submission_id:
            raise RayJobsRestError(200, "Ray submit returned another submission ID")
        return submission_id
