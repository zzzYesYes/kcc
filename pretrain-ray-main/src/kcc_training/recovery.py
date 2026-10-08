"""Infrastructure-free recovery decisions shared by CLI and future controller."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


MAX_RECOVERY_ATTEMPTS = 100


class RecoveryPolicyError(ValueError):
    pass


class FailureScope(str, Enum):
    SOFTWARE = "software"
    NETWORK = "network"
    ARTIFACT = "artifact"
    INFRASTRUCTURE = "infrastructure"
    CHECKPOINT = "checkpoint"
    HARDWARE = "hardware"
    GLOBAL_STALL = "global-stall"
    UNKNOWN = "unknown"
    STOP_REQUESTED = "stop-requested"


class RecoveryAction(str, Enum):
    RETRY_SAME_TOPOLOGY = "retry-same-topology"
    REPLACE_NODES = "replace-nodes"
    STOP = "stop"
    MANUAL_REQUIRED = "manual-required"


@dataclass(frozen=True)
class RecoveryPolicy:
    same_topology_retries: int
    max_replacements: int

    def __post_init__(self) -> None:
        if self.same_topology_retries < 0 or self.max_replacements < 0:
            raise RecoveryPolicyError("recovery budgets must be non-negative")
        maximum_attempt_count(self.max_replacements, self.same_topology_retries)


@dataclass(frozen=True)
class RecoveryEvidence:
    scope: FailureScope
    failed_nodes: tuple[str, ...] = ()
    diagnosis_stable: bool = False
    checkpoint_consistent: bool | None = None
    survivors_healthy_and_idle: bool = False
    spares_healthy_and_idle: int = 0


@dataclass(frozen=True)
class RecoveryDecision:
    action: RecoveryAction
    reason: str
    replacement_count: int = 0


def diagnosis_sample_limit(window_seconds: int, poll_seconds: int) -> int:
    if poll_seconds <= 0:
        raise RecoveryPolicyError("diagnosis poll interval must be positive")
    if window_seconds <= 0:
        return 1
    return 1 + window_seconds // poll_seconds


def maximum_attempt_count(max_replacements: int, same_topology_retries: int) -> int:
    if max_replacements < 0 or same_topology_retries < 0:
        raise RecoveryPolicyError("recovery budgets must be non-negative")
    count = (max_replacements + 1) * (same_topology_retries + 1)
    if count > MAX_RECOVERY_ATTEMPTS:
        raise RecoveryPolicyError(
            f"recovery policy permits {count} attempts; limit is {MAX_RECOVERY_ATTEMPTS}"
        )
    return count


def decide_recovery(
    policy: RecoveryPolicy,
    evidence: RecoveryEvidence,
    *,
    retries_used: int,
    replacements_used: int,
) -> RecoveryDecision:
    if retries_used < 0 or replacements_used < 0:
        raise RecoveryPolicyError("used recovery counters must be non-negative")
    if evidence.scope is FailureScope.STOP_REQUESTED:
        return RecoveryDecision(RecoveryAction.STOP, "operator stop was accepted")
    if evidence.checkpoint_consistent is False:
        return RecoveryDecision(
            RecoveryAction.MANUAL_REQUIRED,
            "active workers disagree on the committed checkpoint",
        )
    if evidence.scope is FailureScope.HARDWARE:
        failed = tuple(sorted(set(evidence.failed_nodes)))
        if not failed or not evidence.diagnosis_stable:
            return RecoveryDecision(
                RecoveryAction.MANUAL_REQUIRED,
                "hardware diagnosis is missing or not stable",
            )
        if not evidence.survivors_healthy_and_idle:
            return RecoveryDecision(
                RecoveryAction.MANUAL_REQUIRED,
                "surviving active nodes are not proven healthy and idle",
            )
        remaining_budget = policy.max_replacements - replacements_used
        if remaining_budget < len(failed):
            return RecoveryDecision(
                RecoveryAction.MANUAL_REQUIRED,
                "replacement budget is insufficient",
            )
        if evidence.spares_healthy_and_idle < len(failed):
            return RecoveryDecision(
                RecoveryAction.MANUAL_REQUIRED,
                "healthy idle spare capacity is insufficient",
            )
        return RecoveryDecision(
            RecoveryAction.REPLACE_NODES,
            "stable node-local hardware fault",
            replacement_count=len(failed),
        )
    if evidence.scope in {
        FailureScope.SOFTWARE,
        FailureScope.NETWORK,
        FailureScope.ARTIFACT,
        FailureScope.INFRASTRUCTURE,
        FailureScope.CHECKPOINT,
        FailureScope.GLOBAL_STALL,
    } and retries_used < policy.same_topology_retries:
        return RecoveryDecision(
            RecoveryAction.RETRY_SAME_TOPOLOGY,
            "failure is not a stable node-local hardware fault",
        )
    return RecoveryDecision(
        RecoveryAction.MANUAL_REQUIRED,
        "failure is ambiguous or the same-topology retry budget is exhausted",
    )

