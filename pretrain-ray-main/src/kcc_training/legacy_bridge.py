"""Explicit conversion from legacy Supervisor evidence to the new domain model.

This module is temporary.  It lets the old orchestration code adopt the pure
decision service without leaking legacy result dictionaries into the future
controller.
"""

from __future__ import annotations

from typing import Any, Mapping

from .recovery import FailureScope, RecoveryEvidence


_FAILURE_SCOPES = {
    "CHECKPOINT_UNAVAILABLE": FailureScope.CHECKPOINT,
    "DRIVER_INTERNAL_FAILURE": FailureScope.SOFTWARE,
    "DRIVER_PROTOCOL_FAILURE": FailureScope.SOFTWARE,
    "INTERRUPTED": FailureScope.NETWORK,
    "TIMEOUT": FailureScope.NETWORK,
    "TRAINING_NO_PROGRESS": FailureScope.GLOBAL_STALL,
}


def recovery_evidence_from_legacy(
    *,
    failure_class: str | None,
    diagnosis_summary: Mapping[str, Any] | None,
    checkpoint_consistent: bool | None,
) -> RecoveryEvidence:
    if diagnosis_summary is not None and diagnosis_summary.get("stable") is True:
        latest = diagnosis_summary.get("latestDiagnosis")
        if isinstance(latest, Mapping):
            raw_failed = latest.get("failedActiveNodes")
            failed = tuple(
                sorted(
                    {
                        item
                        for item in raw_failed
                        if isinstance(item, str) and item
                    }
                )
            ) if isinstance(raw_failed, list) else ()
            raw_non_idle = latest.get("nonIdleActiveNodes")
            non_idle = raw_non_idle if isinstance(raw_non_idle, list) else []
            raw_ambiguous = latest.get("ambiguousActiveNodes")
            ambiguous = raw_ambiguous if isinstance(raw_ambiguous, list) else []
            spare_count = latest.get("availableSpareCount")
            return RecoveryEvidence(
                scope=FailureScope.HARDWARE,
                failed_nodes=failed,
                diagnosis_stable=True,
                checkpoint_consistent=checkpoint_consistent,
                survivors_healthy_and_idle=not non_idle and not ambiguous,
                spares_healthy_and_idle=(
                    spare_count
                    if isinstance(spare_count, int) and not isinstance(spare_count, bool)
                    else 0
                ),
            )
    return RecoveryEvidence(
        scope=_FAILURE_SCOPES.get(failure_class, FailureScope.SOFTWARE),
        checkpoint_consistent=checkpoint_consistent,
    )

