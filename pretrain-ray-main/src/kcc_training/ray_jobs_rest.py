"""Dependency-free Ray Jobs REST transport for the controller."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Callable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


class RayJobsRestError(RuntimeError):
    def __init__(self, status: int | None, message: str) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class RestResponse:
    status: int
    body: bytes


Transport = Callable[[str, str, bytes | None], RestResponse]


class RayJobsRest:
    def __init__(self, transport: Transport | None = None) -> None:
        self._transport_override = transport

    def _transport(self, method: str, url: str, body: bytes | None) -> RestResponse:
        if self._transport_override is not None:
            return self._transport_override(method, url, body)
        request = Request(
            url,
            data=body,
            method=method,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=30) as response:
                return RestResponse(response.status, response.read())
        except HTTPError as error:
            return RestResponse(error.code, error.read())
        except (OSError, URLError) as error:
            raise RayJobsRestError(None, f"Ray Jobs request failed: {error}") from error

    @staticmethod
    def _decode(response: RestResponse, operation: str) -> Mapping[str, Any]:
        try:
            value = json.loads(response.body)
        except json.JSONDecodeError as error:
            raise RayJobsRestError(response.status, f"Ray Jobs {operation} returned invalid JSON") from error
        if not isinstance(value, Mapping):
            raise RayJobsRestError(response.status, f"Ray Jobs {operation} response is not an object")
        return value

    def detail(self, address: str, submission_id: str) -> Mapping[str, Any] | None:
        url = f"{address.rstrip('/')}/api/jobs/{quote(submission_id, safe='')}"
        response = self._transport("GET", url, None)
        if response.status == 404:
            return None
        if response.status != 200:
            raise RayJobsRestError(response.status, f"Ray Jobs detail returned {response.status}")
        value = self._decode(response, "detail")
        returned_id = value.get("submission_id", value.get("submissionId"))
        if returned_id not in (None, submission_id):
            raise RayJobsRestError(200, "Ray Jobs detail returned another submission ID")
        return value

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
            "entrypoint": "python -m kcc_training.runtime.coordinator --spec /etc/kcc/run/run.json",
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

    def status(self, address: str, submission_id: str) -> str:
        detail = self.detail(address, submission_id)
        if detail is None:
            return "NOT_FOUND"
        value = detail.get("status")
        return str(getattr(value, "value", value)).rsplit(".", 1)[-1].upper()

    def stop(self, address: str, submission_id: str) -> None:
        response = self._transport(
            "POST",
            f"{address.rstrip('/')}/api/jobs/{quote(submission_id, safe='')}/stop",
            b"{}",
        )
        if response.status not in (200, 202, 404):
            raise RayJobsRestError(response.status, f"Ray job stop returned {response.status}")

