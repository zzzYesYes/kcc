"""Pure lifecycle rules; infrastructure adapters must not invent transitions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Phase(str, Enum):
    PENDING = "Pending"
    PREFLIGHT = "Preflight"
    STARTING = "Starting"
    HCCL_GATE = "HcclGate"
    RUNNING = "Running"
    RECOVERING = "Recovering"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    STOPPED = "Stopped"
    MANUAL_REQUIRED = "ManualRequired"


TERMINAL_PHASES = frozenset(
    {Phase.SUCCEEDED, Phase.FAILED, Phase.STOPPED, Phase.MANUAL_REQUIRED}
)

_ALLOWED = {
    Phase.PENDING: {Phase.PREFLIGHT, Phase.STOPPED},
    Phase.PREFLIGHT: {Phase.STARTING, Phase.FAILED, Phase.STOPPED},
    Phase.STARTING: {Phase.HCCL_GATE, Phase.RECOVERING, Phase.FAILED, Phase.STOPPED},
    Phase.HCCL_GATE: {Phase.RUNNING, Phase.RECOVERING, Phase.FAILED, Phase.STOPPED},
    Phase.RUNNING: {Phase.SUCCEEDED, Phase.RECOVERING, Phase.FAILED, Phase.STOPPED},
    Phase.RECOVERING: {Phase.STARTING, Phase.MANUAL_REQUIRED, Phase.FAILED, Phase.STOPPED},
}


class InvalidTransition(ValueError):
    pass


def require_transition(current: Phase, target: Phase) -> None:
    if target not in _ALLOWED.get(current, set()):
        raise InvalidTransition(f"invalid training transition: {current.value} -> {target.value}")


@dataclass(frozen=True)
class RecoveryBudget:
    same_topology_retries: int
    max_replacements: int

    def decide(self, *, retries_used: int, replacements_used: int, hardware_fault: bool) -> str:
        if not hardware_fault and retries_used < self.same_topology_retries:
            return "retry-same-topology"
        if hardware_fault and replacements_used < self.max_replacements:
            return "replace-nodes"
        return "manual-required"

