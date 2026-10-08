"""Optional checkpoint evaluation settings for the structured runtime."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Mapping


class EvaluationConfigError(ValueError):
    pass


_ENABLED = {"1", "true", "yes", "on"}
_DISABLED = {"", "0", "false", "no", "off"}


@dataclass(frozen=True)
class EvaluationConfig:
    enabled: bool = False
    command: tuple[str, ...] = ()
    device_id: int = 0
    every_checkpoints: int = 1
    failure_policy: str = "Continue"

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str],
        devices_per_node: int,
    ) -> "EvaluationConfig":
        raw_enabled = environment.get("EVALUATION_ENABLED", "false")
        enabled_value = raw_enabled.strip().lower()
        if enabled_value in _DISABLED:
            # Disabled is a true no-op: stale optional values must not affect the
            # established training path.
            return cls()
        if enabled_value not in _ENABLED:
            raise EvaluationConfigError(
                "EVALUATION_ENABLED must be true or false"
            )

        raw_command = environment.get("EVALUATION_COMMAND_JSON")
        try:
            command = json.loads(raw_command) if raw_command is not None else None
        except json.JSONDecodeError as error:
            raise EvaluationConfigError(
                "EVALUATION_COMMAND_JSON must be a JSON argv array"
            ) from error
        if (
            not isinstance(command, list)
            or not command
            or not all(
                isinstance(item, str) and item and item == item.strip()
                for item in command
            )
        ):
            raise EvaluationConfigError(
                "EVALUATION_COMMAND_JSON must be a non-empty JSON string array"
            )

        raw_device_id = environment.get("EVALUATION_DEVICE_ID", "0")
        raw_every = environment.get("EVALUATION_EVERY_CHECKPOINTS", "1")
        try:
            device_id = int(raw_device_id)
            every_checkpoints = int(raw_every)
        except ValueError as error:
            raise EvaluationConfigError(
                "evaluation device and checkpoint interval must be integers"
            ) from error
        if device_id < 0 or device_id >= devices_per_node:
            raise EvaluationConfigError(
                "EVALUATION_DEVICE_ID must be a visible logical device index"
            )
        if every_checkpoints < 1:
            raise EvaluationConfigError(
                "EVALUATION_EVERY_CHECKPOINTS must be positive"
            )

        raw_policy = environment.get("EVALUATION_FAILURE_POLICY", "Continue")
        policy_value = raw_policy.strip().lower()
        if policy_value == "continue":
            failure_policy = "Continue"
        elif policy_value in {"fail", "failtraining"}:
            failure_policy = "Fail"
        else:
            raise EvaluationConfigError(
                "EVALUATION_FAILURE_POLICY must be Continue or Fail"
            )
        return cls(
            enabled=True,
            command=tuple(command),
            device_id=device_id,
            every_checkpoints=every_checkpoints,
            failure_policy=failure_policy,
        )
