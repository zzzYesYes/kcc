"""Ray Jobs adapter loaded lazily so the control package has no Ray dependency."""

from __future__ import annotations

import shlex
from typing import Any, Mapping, Sequence


class RayJobsError(RuntimeError):
    pass


def _status_text(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw).rsplit(".", 1)[-1].upper()


class RayJobsAdapter:
    def __init__(self, client_factory: Any | None = None) -> None:
        self._client_factory = client_factory

    def _client(self, address: str) -> Any:
        factory = self._client_factory
        if factory is None:
            try:
                from ray.job_submission import JobSubmissionClient
            except ImportError as error:
                raise RayJobsError(f"Ray Jobs SDK is unavailable: {error}") from error
            factory = JobSubmissionClient
        try:
            return factory(address)
        except Exception as error:
            raise RayJobsError(f"cannot connect to Ray Jobs API: {error}") from error

    def submit_once(
        self,
        address: str,
        submission_id: str,
        command: Sequence[str],
        metadata: Mapping[str, str] | None = None,
    ) -> str:
        if not command:
            raise RayJobsError("Ray job command must not be empty")
        client = self._client(address)
        try:
            returned = client.submit_job(
                entrypoint=shlex.join(tuple(command)),
                submission_id=submission_id,
                metadata=dict(metadata or {}),
            )
            if returned != submission_id:
                raise RayJobsError("Ray returned a different submission ID")
            return str(returned)
        except RayJobsError:
            raise
        except Exception as submit_error:
            try:
                client.get_job_status(submission_id)
            except Exception:
                raise RayJobsError(f"Ray job submission failed: {submit_error}") from submit_error
            return submission_id

    def status(self, address: str, submission_id: str) -> str:
        try:
            return _status_text(self._client(address).get_job_status(submission_id))
        except RayJobsError:
            raise
        except Exception as error:
            raise RayJobsError(f"cannot query Ray job status: {error}") from error

    def stop(self, address: str, submission_id: str) -> None:
        try:
            self._client(address).stop_job(submission_id)
        except RayJobsError:
            raise
        except Exception as error:
            raise RayJobsError(f"cannot stop Ray job: {error}") from error

