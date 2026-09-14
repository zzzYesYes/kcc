"""TrainingRun reconciler and polling controller entrypoint."""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime, timezone
import json
import os
import socket
import sys
import time
from typing import Any, Callable, Mapping, Protocol, Sequence

from .api_v1beta1 import ApiValidationError, Recipe, Run, RuntimeProfile
from .kube_api import KubernetesApi, KubernetesApiError, KubernetesOwnershipError, LeaseLock, core_namespaced_path, namespaced_path, utc_now
from .ray_jobs_rest import RayJobsRest, RayJobsRestError
from .raycluster import attempt_name, render_attempt, render_control
from .recovery import FailureScope, RecoveryAction, RecoveryEvidence, RecoveryPolicy, decide_recovery


GROUP = "training.kcc.io"
VERSION = "v1beta1"
TERMINAL = {"Succeeded", "Stopped", "ManualRequired"}
_PERMANENT_HTTP_STATUS = {400, 401, 403, 409, 422}
PROGRESS_SCHEMA = "kcc-runtime-progress/v1"


class HealthProvider(Protocol):
    def observe(self, nodes: Sequence[str]) -> Mapping[str, Any]: ...


class ControllerError(RuntimeError):
    pass


class RetryableControllerError(RuntimeError):
    """A reconciliation dependency is temporarily unable to provide evidence."""


def _condition(condition_type: str, status: str, reason: str, message: str) -> dict[str, Any]:
    return {
        "type": condition_type,
        "status": status,
        "reason": reason,
        "message": message[:1000],
        "lastTransitionTime": utc_now(),
    }


def _status(current: Mapping[str, Any], **changes: Any) -> dict[str, Any]:
    result = dict(current.get("status")) if isinstance(current.get("status"), Mapping) else {}
    result.update(changes)
    result["updatedAt"] = utc_now()
    return result


def _cluster_ready(cluster: Mapping[str, Any], workers: int) -> bool:
    status = cluster.get("status")
    if not isinstance(status, Mapping):
        return False
    state = str(status.get("state", "")).lower()
    ready_workers = status.get("readyWorkerReplicas")
    return state == "ready" and ready_workers == workers


def _phase(resource: Mapping[str, Any]) -> str:
    status = resource.get("status")
    if not isinstance(status, Mapping):
        return "Pending"
    value = status.get("phase", "Pending")
    return str(value) if value else "Pending"


def _elapsed_since(value: Any, now: float) -> float | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None
    return max(0.0, now - timestamp)


def _trusted_result(document: Mapping[str, Any] | None, run: Run, attempt: int) -> Mapping[str, Any] | None:
    if document is None:
        return None
    metadata = document.get("metadata")
    data = document.get("data")
    if not isinstance(metadata, Mapping) or not isinstance(data, Mapping):
        raise ControllerError("runtime result ConfigMap is malformed")
    annotations = metadata.get("annotations")
    if (
        not isinstance(annotations, Mapping)
        or annotations.get("training.kcc.io/run-uid") != run.identity.uid
        or annotations.get("training.kcc.io/attempt") != str(attempt)
    ):
        raise ControllerError("runtime result ConfigMap ownership differs")
    raw = data.get("result.json")
    if not isinstance(raw, str) or len(raw.encode()) > 1024 * 1024:
        raise ControllerError("runtime result is missing or too large")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ControllerError(f"runtime result is invalid JSON: {error}") from error
    if (
        not isinstance(value, Mapping)
        or value.get("schemaVersion") != "kcc-runtime-result/v1"
        or value.get("runUid") != run.identity.uid
        or value.get("attempt") != attempt
        or value.get("status") not in {"PASS", "FAIL", "STOPPED"}
        or not isinstance(value.get("checkpointConsistent"), bool)
    ):
        raise ControllerError("runtime result fields are invalid")

    if value["status"] == "STOPPED":
        checkpoint = value.get("checkpoint")
        request_generation = value.get("stopRequestGeneration")
        stop_reason = value.get("stopReason")
        if (
            isinstance(request_generation, bool)
            or not isinstance(request_generation, int)
            or request_generation < 1
        ):
            raise ControllerError("runtime stop evidence generation is invalid")
        if stop_reason == "AfterCheckpoint":
            baseline = value.get("stopBaselineIteration")
            iteration = (
                checkpoint.get("iteration")
                if isinstance(checkpoint, Mapping)
                else None
            )
            if (
                value.get("checkpointConsistent") is not True
                or value.get("checkpointAvailable") is not True
                or not isinstance(checkpoint, Mapping)
                or isinstance(baseline, bool)
                or not isinstance(baseline, int)
                or baseline < 0
                or isinstance(iteration, bool)
                or not isinstance(iteration, int)
                or iteration <= baseline
            ):
                raise ControllerError("runtime graceful-stop evidence is invalid")
        elif stop_reason == "Immediate":
            cleanup = value.get("checkpointCleanup")
            if (
                value.get("checkpointConsistent") is not True
                or not isinstance(cleanup, Mapping)
                or cleanup.get("completed") is not True
            ):
                raise ControllerError("runtime immediate-stop cleanup evidence is invalid")
        else:
            raise ControllerError("runtime stop reason is invalid")

    failed_nodes = value.get("failedNodes", [])
    failure_scope = value.get("failureScope")
    if (
        not isinstance(failed_nodes, list)
        or not all(isinstance(item, str) and item for item in failed_nodes)
        or (failure_scope is not None and not isinstance(failure_scope, str))
    ):
        raise ControllerError("runtime failure evidence is invalid")
    return value


def _trusted_progress(
    document: Mapping[str, Any] | None,
    run: Run,
    attempt: int,
) -> Mapping[str, Any] | None:
    if document is None:
        return None
    metadata = document.get("metadata")
    data = document.get("data")
    if not isinstance(metadata, Mapping) or not isinstance(data, Mapping):
        return None
    annotations = metadata.get("annotations")
    if (
        not isinstance(annotations, Mapping)
        or annotations.get("training.kcc.io/run-uid") != run.identity.uid
        or annotations.get("training.kcc.io/attempt") != str(attempt)
    ):
        return None
    raw = data.get("progress.json")
    if not isinstance(raw, str) or len(raw.encode()) > 128 * 1024:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if (
        not isinstance(value, Mapping)
        or value.get("schemaVersion") != PROGRESS_SCHEMA
        or value.get("runName") != run.identity.name
        or value.get("runUid") != run.identity.uid
        or value.get("attempt") != attempt
        or not isinstance(value.get("stage"), str)
        or not isinstance(value.get("status"), str)
        or not isinstance(value.get("message"), str)
        or not isinstance(value.get("updatedAt"), str)
    ):
        return None
    return dict(value)


class Reconciler:
    def __init__(
        self,
        api: KubernetesApi,
        jobs: RayJobsRest,
        *,
        runtime_service_account: str,
        health: HealthProvider | None = None,
        stable_diagnosis_samples: int = 2,
        start_timeout_seconds: int = 900,
        result_timeout_seconds: int = 120,
        clock: Callable[[], float] = time.time,
        profile_loader: Callable[[Mapping[str, Any]], RuntimeProfile] | None = None,
        recipe_loader: Callable[[Mapping[str, Any]], Recipe] | None = None,
        renderer: Callable[..., tuple[Mapping[str, Any], Mapping[str, Any]]] | None = None,
        result_validator: Callable[[Mapping[str, Any] | None, Run, int], Mapping[str, Any] | None] | None = None,
    ) -> None:
        self.api = api
        self.jobs = jobs
        self.runtime_service_account = runtime_service_account
        self.health = health
        self.stable_diagnosis_samples = max(1, stable_diagnosis_samples)
        self.start_timeout_seconds = max(1, start_timeout_seconds)
        self.result_timeout_seconds = max(1, result_timeout_seconds)
        self.clock = clock
        self.profile_loader = profile_loader or RuntimeProfile.from_resource
        self.recipe_loader = recipe_loader or Recipe.from_resource
        self.renderer = renderer or render_attempt
        self.result_validator = result_validator or _trusted_result

    def _now_text(self) -> str:
        return datetime.fromtimestamp(self.clock(), timezone.utc).isoformat().replace("+00:00", "Z")

    def _resource(self, namespace: str, plural: str, name: str) -> Mapping[str, Any]:
        path = namespaced_path(GROUP, VERSION, namespace, plural, name)
        value = self.api.get(path)
        if value is None:
            raise RetryableControllerError(f"referenced {plural}/{name} does not exist yet")
        return value

    def _write_status(self, resource: Mapping[str, Any], status: Mapping[str, Any]) -> Mapping[str, Any]:
        metadata = resource["metadata"]
        path = namespaced_path(GROUP, VERSION, metadata["namespace"], "trainingruns", metadata["name"])
        return self.api.update_status(path, resource, status)

    def _mark_manual(self, resource: Mapping[str, Any], error: Exception) -> str:
        status = _status(
            resource,
            phase="ManualRequired",
            observedGeneration=resource.get("metadata", {}).get("generation"),
            conditions=[_condition("Ready", "False", "ReconcileRefused", str(error))],
        )
        self._write_status(resource, status)
        return "ManualRequired"

    def _record_retry(self, resource: Mapping[str, Any], error: Exception) -> str:
        phase = _phase(resource)
        self._write_status(
            resource,
            _status(
                resource,
                phase=phase,
                observedGeneration=resource.get("metadata", {}).get("generation"),
                conditions=[_condition("Ready", "False", "ReconcileRetrying", str(error))],
            ),
        )
        return phase

    def _cleanup_cluster(self, run: Run, cluster_name: str) -> bool:
        """Delete one owned cluster and report whether it is already absent."""
        path = namespaced_path("ray.io", "v1", run.identity.namespace, "rayclusters", cluster_name)
        cluster = self.api.get(path)
        if cluster is None:
            return True
        metadata = cluster.get("metadata")
        annotations = metadata.get("annotations") if isinstance(metadata, Mapping) else None
        uid = metadata.get("uid") if isinstance(metadata, Mapping) else None
        if not isinstance(annotations, Mapping) or annotations.get("training.kcc.io/run-uid") != run.identity.uid:
            raise ControllerError("refusing to delete a RayCluster owned by another run")
        if not isinstance(uid, str) or not uid:
            raise ControllerError("RayCluster has no UID for preconditioned delete")
        self.api.delete_owned(path, uid)
        return False


    def _upsert_runtime_control(
        self,
        run: Run,
        attempt: int,
        action: str,
        request_generation: int,
    ) -> None:
        document = render_control(
            run,
            attempt=attempt,
            action=action,
            request_generation=request_generation,
        )
        collection = core_namespaced_path(run.identity.namespace, "configmaps")
        path = core_namespaced_path(
            run.identity.namespace,
            "configmaps",
            document["metadata"]["name"],
        )
        self.api.upsert(collection, path, document)

    def _dependency_ready(
        self,
        resource: Mapping[str, Any],
        run: Run,
        current: Mapping[str, Any],
    ) -> bool:
        """Wait for the preceding run to succeed and release its RayCluster."""
        dependency_name = run.depends_on
        if dependency_name is None:
            return True

        dependency_path = namespaced_path(
            GROUP,
            VERSION,
            run.identity.namespace,
            "trainingruns",
            dependency_name,
        )
        dependency = self.api.get(dependency_path)
        reason = "WaitingForDependency"
        if dependency is None:
            message = f"waiting for TrainingRun {dependency_name} to be created"
        else:
            dependency_phase = _phase(dependency)
            if dependency_phase != "Succeeded":
                message = (
                    f"waiting for TrainingRun {dependency_name} "
                    f"to succeed; current phase is {dependency_phase}"
                )
            else:
                dependency_status = (
                    dependency.get("status")
                    if isinstance(dependency.get("status"), Mapping)
                    else {}
                )
                cluster_names = tuple(
                    dict.fromkeys(
                        name
                        for name in (
                            dependency_status.get("clusterName"),
                            dependency_status.get("cleanupClusterName"),
                        )
                        if isinstance(name, str) and name
                    )
                )
                remaining = [
                    name
                    for name in cluster_names
                    if self.api.get(
                        namespaced_path(
                            "ray.io",
                            "v1",
                            run.identity.namespace,
                            "rayclusters",
                            name,
                        )
                    )
                    is not None
                ]
                if not remaining:
                    return True
                reason = "WaitingForDependencyCleanup"
                message = (
                    f"waiting for TrainingRun {dependency_name} "
                    f"to release RayCluster {remaining[0]}"
                )

        conditions = current.get("conditions")
        existing = (
            conditions[0]
            if isinstance(conditions, list)
            and conditions
            and isinstance(conditions[0], Mapping)
            else {}
        )
        if (
            str(current.get("phase", "Pending")) == "Queued"
            and current.get("observedGeneration") == run.identity.generation
            and existing.get("reason") == reason
            and existing.get("message") == message
        ):
            return False
        self._write_status(
            resource,
            _status(
                resource,
                phase="Queued",
                observedGeneration=run.identity.generation,
                conditions=[
                    _condition(
                        "Ready",
                        "False",
                        reason,
                        message,
                    )
                ],
            ),
        )
        return False

    def reconcile(self, resource: Mapping[str, Any]) -> str:
        try:
            run = Run.from_resource(resource)
        except ApiValidationError as error:
            return self._mark_manual(resource, error)

        phase = _phase(resource)
        if phase in TERMINAL:
            cluster_name = resource.get("status", {}).get("clusterName")
            if isinstance(cluster_name, str) and cluster_name:
                try:
                    self._cleanup_cluster(run, cluster_name)
                except (ControllerError, KubernetesApiError):
                    # Terminal state is authoritative; safe owned cleanup is retried on the next poll.
                    pass
            return phase

        try:
            profile = self.profile_loader(
                self._resource(run.identity.namespace, "trainingruntimeprofiles", run.profile_name)
            )
            if (
                run.devices_per_node is not None
                and profile.physical_device_ids
                and run.devices_per_node != profile.devices_per_node
            ):
                raise ControllerError(
                    "devicesPerNode cannot override a profile with fixed physicalDeviceIDs"
                )
            profile = replace(
                profile,
                head_image=run.head_image or profile.head_image,
                worker_image=run.worker_image or profile.worker_image,
                devices_per_node=run.devices_per_node or profile.devices_per_node,
            )
            recipe = self.recipe_loader(
                self._resource(run.identity.namespace, "trainingrecipes", run.recipe_name)
            )
            return self._reconcile_valid(resource, run, profile, recipe)
        except ApiValidationError as error:
            return self._mark_manual(resource, error)
        except RetryableControllerError as error:
            return self._record_retry(resource, error)
        except ControllerError as error:
            return self._mark_manual(resource, error)
        except KubernetesOwnershipError as error:
            return self._mark_manual(resource, error)
        except KubernetesApiError as error:
            if error.status in {400, 401, 403, 422}:
                return self._mark_manual(resource, error)
            # A failed Kubernetes operation did not commit a new controller state.
            # Retry from the current resourceVersion on the next list cycle.
            return phase
        except RayJobsRestError as error:
            if error.status in _PERMANENT_HTTP_STATUS:
                return self._mark_manual(resource, error)
            return self._record_retry(resource, error)

    def _reconcile_valid(
        self,
        resource: Mapping[str, Any],
        run: Run,
        profile: RuntimeProfile,
        recipe: Recipe,
    ) -> str:
        current = resource.get("status") if isinstance(resource.get("status"), Mapping) else {}
        phase = str(current.get("phase", "Pending"))

        if phase == "Stopping":
            if not run.suspended:
                return self._cancel_checkpoint_stop(resource, run, current)
            if run.suspend_mode == "Immediate":
                return self._reconcile_immediate_stop(resource, run, current)
            return self._reconcile_checkpoint_stop(resource, run, current)
        if run.suspended:
            if run.suspend_mode == "AfterCheckpoint" and phase == "Running":
                return self._request_checkpoint_stop(resource, run, current)
            if run.suspend_mode == "Immediate" and phase == "Running":
                return self._request_immediate_stop(resource, run, current)
            return self._suspend(resource, run, current, phase)
        if phase == "Suspended":
            return self._resume(resource, run, current)
        if phase in {"Pending", "Queued"} and not self._dependency_ready(resource, run, current):
            return "Queued"
        profile_pool = set((*profile.active_nodes, *profile.spare_nodes))
        requested_active = run.active_nodes or profile.active_nodes[: run.workers]
        requested_spares = run.spare_nodes if run.active_nodes else profile.spare_nodes
        if len(requested_active) != run.workers:
            raise ControllerError("selected active node count differs from workers")
        if len(requested_active) != len(set(requested_active)):
            raise ControllerError("selected active nodes contain duplicates")
        if set(requested_active) & set(requested_spares):
            raise ControllerError("selected active and spare nodes overlap")
        if not set((*requested_active, *requested_spares)) <= profile_pool:
            raise ControllerError("selected nodes are outside the RuntimeProfile pool")
        if run.max_replacements > len(requested_spares):
            raise ControllerError("recovery budget exceeds the spare pool")

        attempt = int(current.get("attempt", 0))
        active = tuple(current.get("activeNodes", requested_active))
        spares = tuple(current.get("spareNodes", requested_spares))
        expected_name = attempt_name(run.identity.name, attempt)
        persisted_name = current.get("clusterName")
        cluster_name = persisted_name if isinstance(persisted_name, str) and persisted_name else expected_name

        if phase == "Recovering":
            cleanup_name = current.get("cleanupClusterName")
            if isinstance(cleanup_name, str) and cleanup_name:
                if not self._cleanup_cluster(run, cleanup_name):
                    return "Recovering"
            return self._provision_attempt(resource, run, profile, recipe, current, attempt, active, spares)
        if phase in {"Pending", "Queued"}:
            return self._provision_attempt(resource, run, profile, recipe, current, attempt, active, spares)

        cluster_path = namespaced_path("ray.io", "v1", run.identity.namespace, "rayclusters", cluster_name)
        cluster = self.api.get(cluster_path)
        if phase == "Starting":
            return self._reconcile_starting(
                resource, run, profile, recipe, current, attempt, active, spares, cluster_name, cluster
            )
        if phase != "Running":
            raise ControllerError(f"unsupported persisted phase: {phase}")
        return self._reconcile_running(
            resource, run, current, attempt, active, spares, cluster_name, cluster
        )


    def _request_checkpoint_stop(
        self,
        resource: Mapping[str, Any],
        run: Run,
        current: Mapping[str, Any],
    ) -> str:
        attempt = int(current.get("attempt", 0))
        requested = _status(
            resource,
            phase="Stopping",
            observedGeneration=run.identity.generation,
            suspendedFrom="Running",
            stopRequestGeneration=run.identity.generation,
            stopRequestedAt=self._now_text(),
            resultWaitStartedAt="",
            conditions=[
                _condition(
                    "Ready",
                    "False",
                    "WaitingForCheckpoint",
                    "waiting for the next consistent checkpoint",
                )
            ],
        )
        # Persist the stop intent before changing the runtime control channel.
        self._write_status(resource, requested)
        self._upsert_runtime_control(
            run,
            attempt,
            "StopAfterCheckpoint",
            run.identity.generation,
        )
        return "Stopping"

    def _request_immediate_stop(
        self,
        resource: Mapping[str, Any],
        run: Run,
        current: Mapping[str, Any],
    ) -> str:
        attempt = int(current.get("attempt", 0))
        requested = _status(
            resource,
            phase="Stopping",
            observedGeneration=run.identity.generation,
            suspendedFrom="Running",
            stopRequestGeneration=run.identity.generation,
            conditions=[
                _condition(
                    "Ready",
                    "False",
                    "StoppingImmediate",
                    "stopping workers and deleting checkpoints created by this attempt",
                )
            ],
        )
        self._write_status(resource, requested)
        self._upsert_runtime_control(
            run,
            attempt,
            "StopImmediate",
            run.identity.generation,
        )
        return "Stopping"

    def _reconcile_immediate_stop(
        self,
        resource: Mapping[str, Any],
        run: Run,
        current: Mapping[str, Any],
    ) -> str:
        if current.get("stopRequestGeneration") != run.identity.generation:
            return self._request_immediate_stop(resource, run, current)
        attempt = int(current.get("attempt", 0))
        cluster_name = str(current.get("clusterName", ""))
        if not cluster_name:
            raise ControllerError("immediate-stop status lacks RayCluster identity")
        self._upsert_runtime_control(
            run,
            attempt,
            "StopImmediate",
            run.identity.generation,
        )
        result = self.result_validator(
            self.api.get(
                core_namespaced_path(
                    run.identity.namespace,
                    "configmaps",
                    f"{cluster_name}-result",
                )
            ),
            run,
            attempt,
        )
        if result is None:
            return "Stopping"
        if result.get("status") == "PASS":
            condition_reason = "CompletedBeforeImmediateStop"
            condition_message = "training completed before the immediate stop request"
        elif result.get("status") == "STOPPED" and result.get("stopReason") == "Immediate":
            cleanup = result.get("checkpointCleanup", {})
            condition_reason = "SuspendedImmediate"
            condition_message = (
                "immediate stop completed; unapproved checkpoints were deleted "
                f"(removed {cleanup.get('removedEntries', 0)})"
            )
        else:
            raise ControllerError("runtime failed while processing immediate stop")
        suspended = _status(
            resource,
            phase="Suspended",
            observedGeneration=run.identity.generation,
            suspendedFrom="Running",
            checkpoint=result.get("checkpoint"),
            conditions=[
                _condition("Ready", "False", condition_reason, condition_message)
            ],
        )
        self._write_status(resource, suspended)
        self._cleanup_cluster(run, cluster_name)
        return "Suspended"

    def _cancel_checkpoint_stop(
        self,
        resource: Mapping[str, Any],
        run: Run,
        current: Mapping[str, Any],
    ) -> str:
        attempt = int(current.get("attempt", 0))
        self._upsert_runtime_control(
            run,
            attempt,
            "Continue",
            run.identity.generation,
        )
        self._write_status(
            resource,
            _status(
                resource,
                phase="Running",
                observedGeneration=run.identity.generation,
                resultWaitStartedAt="",
                conditions=[
                    _condition(
                        "Ready",
                        "False",
                        "CheckpointStopCancelled",
                        "checkpoint stop request was cancelled",
                    )
                ],
            ),
        )
        return "Running"

    def _reconcile_checkpoint_stop(
        self,
        resource: Mapping[str, Any],
        run: Run,
        current: Mapping[str, Any],
    ) -> str:
        if current.get("stopRequestGeneration") != run.identity.generation:
            return self._request_checkpoint_stop(resource, run, current)
        attempt = int(current.get("attempt", 0))
        cluster_name = str(current.get("clusterName", ""))
        if not cluster_name:
            raise ControllerError("stopping status lacks RayCluster identity")
        self._upsert_runtime_control(
            run,
            attempt,
            "StopAfterCheckpoint",
            run.identity.generation,
        )
        cluster_path = namespaced_path(
            "ray.io", "v1", run.identity.namespace, "rayclusters", cluster_name
        )
        if self.api.get(cluster_path) is None:
            raise ControllerError("RayCluster disappeared during checkpoint stop")
        result_name = f"{cluster_name}-result"
        progress = _trusted_progress(
            self.api.get(
                core_namespaced_path(
                    run.identity.namespace,
                    "configmaps",
                    f"{cluster_name}-progress",
                )
            ),
            run,
            attempt,
        )
        if progress is not None and progress != current.get("progress"):
            self._write_status(
                resource,
                _status(
                    resource,
                    progress=progress,
                    conditions=[
                        _condition(
                            "Ready",
                            "False",
                            "RuntimeProgress",
                            f"{progress['stage']}: {progress['message']}",
                        )
                    ],
                ),
            )
            return "Stopping"
        result = self.result_validator(
            self.api.get(
                core_namespaced_path(
                    run.identity.namespace, "configmaps", result_name
                )
            ),
            run,
            attempt,
        )
        if result is None:
            address = current.get("rayAddress")
            submission = current.get("submissionId")
            if (
                not isinstance(address, str)
                or not address
                or not isinstance(submission, str)
                or not submission
            ):
                raise ControllerError("stopping status lacks Ray job identity")
            job_status = self.jobs.status(address, submission)
            if job_status not in {"SUCCEEDED", "FAILED", "STOPPED", "NOT_FOUND"}:
                return "Stopping"
            elapsed = _elapsed_since(
                current.get("resultWaitStartedAt"), self.clock()
            )
            if elapsed is None:
                self._write_status(
                    resource,
                    _status(
                        resource,
                        resultWaitStartedAt=self._now_text(),
                        conditions=[
                            _condition(
                                "Ready",
                                "False",
                                "RuntimeStopResultPending",
                                result_name,
                            )
                        ],
                    ),
                )
                return "Stopping"
            if elapsed < self.result_timeout_seconds:
                return "Stopping"
            raise ControllerError(
                "runtime checkpoint-stop result did not become available"
            )

        if result["status"] == "PASS":
            self._write_status(
                resource,
                _status(
                    resource,
                    phase="Succeeded",
                    completionTime=utc_now(),
                    checkpoint=result.get("checkpoint"),
                    outputArtifact=result.get("outputArtifact"),
                    conditions=[
                        _condition(
                            "Ready",
                            "True",
                            "TrainingSucceeded",
                            "training completed before the checkpoint stop",
                        )
                    ],
                ),
            )
            return "Succeeded"
        if result["status"] != "STOPPED":
            raise ControllerError("training failed while waiting for checkpoint stop")
        if result.get("stopRequestGeneration") != current.get(
            "stopRequestGeneration"
        ):
            raise ControllerError("runtime stop result belongs to another request")

        suspended = _status(
            resource,
            phase="Suspended",
            observedGeneration=run.identity.generation,
            suspendedFrom=str(current.get("suspendedFrom", "Running")),
            completionTime="",
            checkpoint=result["checkpoint"],
            stopBaselineIteration=result["stopBaselineIteration"],
            conditions=[
                _condition(
                    "Ready",
                    "False",
                    "SuspendedAfterCheckpoint",
                    f"checkpoint iteration {result['checkpoint']['iteration']} committed",
                )
            ],
        )
        # Persist the verified checkpoint before deleting compute resources.
        self._write_status(resource, suspended)
        self._cleanup_cluster(run, cluster_name)
        return "Suspended"

    def _suspend(
        self,
        resource: Mapping[str, Any],
        run: Run,
        current: Mapping[str, Any],
        phase: str,
    ) -> str:
        newly_suspended = phase != "Suspended"
        suspended_from = (
            str(current.get("suspendedFrom", "Running"))
            if phase == "Stopping"
            else phase
        )
        if newly_suspended:
            suspended_status = _status(
                resource,
                phase="Suspended",
                observedGeneration=run.identity.generation,
                suspendedFrom=suspended_from,
                conditions=[_condition("Ready", "False", "Suspended", "operator requested suspension")],
            )
            # Persist intent before issuing either external stop operation.
            self._write_status(resource, suspended_status)
            current = suspended_status

        address = current.get("rayAddress")
        submission = current.get("submissionId")
        if newly_suspended and isinstance(address, str) and address and isinstance(submission, str) and submission:
            try:
                self.jobs.stop(address, submission)
            except RayJobsRestError:
                # Cluster deletion below is the authoritative stop mechanism.
                pass
        cluster_names = tuple(
            dict.fromkeys(
                name
                for name in (current.get("clusterName"), current.get("cleanupClusterName"))
                if isinstance(name, str) and name
            )
        )
        for cluster_name in cluster_names:
            self._cleanup_cluster(run, cluster_name)
        return "Suspended"

    def _resume(self, resource: Mapping[str, Any], run: Run, current: Mapping[str, Any]) -> str:
        cluster_names = tuple(
            dict.fromkeys(
                name
                for name in (current.get("clusterName"), current.get("cleanupClusterName"))
                if isinstance(name, str) and name
            )
        )
        all_absent = True
        for cluster_name in cluster_names:
            if not self._cleanup_cluster(run, cluster_name):
                all_absent = False
        if not all_absent:
            return "Suspended"

        previous = str(current.get("suspendedFrom", "Pending"))
        old_attempt = int(current.get("attempt", 0))
        if previous == "Recovering":
            next_attempt = old_attempt
            next_phase = "Recovering"
        elif previous in {"Starting", "Running"}:
            next_attempt = old_attempt + 1
            next_phase = "Recovering"
        else:
            next_attempt = old_attempt
            next_phase = "Pending"
        self._write_status(
            resource,
            _status(
                resource,
                phase=next_phase,
                attempt=next_attempt,
                clusterName=attempt_name(run.identity.name, next_attempt),
                cleanupClusterName="",
                rayAddress="",
                submissionId="",
                observedGeneration=run.identity.generation,
                conditions=[_condition("Ready", "False", "Resuming", "suspension was removed")],
            ),
        )
        return next_phase

    def _provision_attempt(
        self,
        resource: Mapping[str, Any],
        run: Run,
        profile: RuntimeProfile,
        recipe: Recipe,
        current: Mapping[str, Any],
        attempt: int,
        active: tuple[str, ...],
        spares: tuple[str, ...],
    ) -> str:
        configmap, cluster = self.renderer(
            run,
            profile,
            recipe,
            attempt=attempt,
            active_nodes=active,
            runtime_service_account=self.runtime_service_account,
        )
        self._upsert_runtime_control(
            run,
            attempt,
            "Continue",
            run.identity.generation,
        )
        cluster_name = attempt_name(run.identity.name, attempt)
        cm_collection = core_namespaced_path(run.identity.namespace, "configmaps")
        self.api.upsert(
            cm_collection,
            core_namespaced_path(run.identity.namespace, "configmaps", configmap["metadata"]["name"]),
            configmap,
        )
        ray_collection = namespaced_path("ray.io", "v1", run.identity.namespace, "rayclusters")
        self.api.upsert(
            ray_collection,
            namespaced_path("ray.io", "v1", run.identity.namespace, "rayclusters", cluster_name),
            cluster,
        )
        self._write_status(
            resource,
            _status(
                resource,
                phase="Starting",
                observedGeneration=run.identity.generation,
                attempt=attempt,
                activeNodes=list(active),
                spareNodes=list(spares),
                clusterName=cluster_name,
                cleanupClusterName="",
                rayAddress="",
                submissionId="",
                startTime=self._now_text(),
                resultWaitStartedAt="",
                retriesUsed=int(current.get("retriesUsed", 0)),
                replacementsUsed=int(current.get("replacementsUsed", 0)),
                conditions=[_condition("Ready", "False", "RayClusterStarting", cluster_name)],
            ),
        )
        return "Starting"

    def _reconcile_starting(
        self,
        resource: Mapping[str, Any],
        run: Run,
        profile: RuntimeProfile,
        recipe: Recipe,
        current: Mapping[str, Any],
        attempt: int,
        active: tuple[str, ...],
        spares: tuple[str, ...],
        cluster_name: str,
        cluster: Mapping[str, Any] | None,
    ) -> str:
        if cluster is None:
            # Recreate an externally removed or not-yet-created attempt idempotently.
            return self._provision_attempt(resource, run, profile, recipe, current, attempt, active, spares)
        if not _cluster_ready(cluster, run.workers):
            elapsed = _elapsed_since(current.get("startTime"), self.clock())
            if elapsed is None:
                self._write_status(resource, _status(resource, startTime=self._now_text()))
                return "Starting"
            if elapsed >= self.start_timeout_seconds:
                return self._recover(
                    resource,
                    run,
                    current,
                    {
                        "failureScope": "network",
                        "failedNodes": [],
                        "checkpointConsistent": None,
                    },
                    active,
                    spares,
                    cluster_name,
                )
            return "Starting"
        address = f"http://{cluster_name}-head-svc.{run.identity.namespace}.svc:8265"
        submission = cluster_name
        self.jobs.submit_once(
            address,
            submission,
            (*(run.command or recipe.command), *run.command_arguments),
            metadata={"runUid": run.identity.uid, "attempt": str(attempt)},
        )
        self._write_status(
            resource,
            _status(
                resource,
                phase="Running",
                rayAddress=address,
                submissionId=submission,
                runningStartedAt=self._now_text(),
                conditions=[_condition("Ready", "False", "TrainingRunning", submission)],
            ),
        )
        return "Running"

    def _reconcile_running(
        self,
        resource: Mapping[str, Any],
        run: Run,
        current: Mapping[str, Any],
        attempt: int,
        active: tuple[str, ...],
        spares: tuple[str, ...],
        cluster_name: str,
        cluster: Mapping[str, Any] | None,
    ) -> str:
        if cluster is None:
            return self._recover(
                resource,
                run,
                current,
                {"failureScope": "network", "failedNodes": [], "checkpointConsistent": None},
                active,
                spares,
                cluster_name,
            )
        address = current.get("rayAddress")
        submission = current.get("submissionId")
        if not isinstance(address, str) or not address or not isinstance(submission, str) or not submission:
            raise ControllerError("running status lacks Ray job identity")
        result_name = f"{cluster_name}-result"
        progress = _trusted_progress(
            self.api.get(
                core_namespaced_path(
                    run.identity.namespace,
                    "configmaps",
                    f"{cluster_name}-progress",
                )
            ),
            run,
            attempt,
        )
        if progress is not None and progress != current.get("progress"):
            self._write_status(
                resource,
                _status(
                    resource,
                    progress=progress,
                    conditions=[
                        _condition(
                            "Ready",
                            "False",
                            "RuntimeProgress",
                            f"{progress['stage']}: {progress['message']}",
                        )
                    ],
                ),
            )
            return "Running"
        result = self.result_validator(
            self.api.get(core_namespaced_path(run.identity.namespace, "configmaps", result_name)),
            run,
            attempt,
        )
        if result is not None:
            # The owned result ConfigMap survives Dashboard restarts and is the
            # authoritative terminal record for this fixed attempt identity.
            job_status = {
                "PASS": "SUCCEEDED",
                "FAIL": "FAILED",
                "STOPPED": "STOPPED",
            }[str(result["status"])]
        else:
            job_status = self.jobs.status(address, submission)
            if job_status == "NOT_FOUND":
                return self._recover(
                    resource,
                    run,
                    current,
                    {"failureScope": "infrastructure", "failedNodes": [], "checkpointConsistent": None},
                    active,
                    spares,
                    cluster_name,
                )
            if job_status not in {"SUCCEEDED", "FAILED", "STOPPED"}:
                return "Running"
        if result is None:
            elapsed = _elapsed_since(current.get("resultWaitStartedAt"), self.clock())
            if elapsed is None:
                self._write_status(
                    resource,
                    _status(
                        resource,
                        resultWaitStartedAt=self._now_text(),
                        conditions=[_condition("Ready", "False", "RuntimeResultPending", result_name)],
                    ),
                )
                return "Running"
            if elapsed < self.result_timeout_seconds:
                return "Running"
            return self._recover(
                resource,
                run,
                current,
                {"failureScope": "software", "failedNodes": [], "checkpointConsistent": None},
                active,
                spares,
                cluster_name,
            )
        if result["status"] == "PASS" and job_status == "SUCCEEDED":
            self._write_status(
                resource,
                _status(
                    resource,
                    phase="Succeeded",
                    completionTime=utc_now(),
                    checkpoint=result.get("checkpoint"),
                    outputArtifact=result.get("outputArtifact"),
                    conditions=[_condition("Ready", "True", "TrainingSucceeded", submission)],
                ),
            )
            return "Succeeded"
        if result["status"] == "STOPPED":
            next_attempt = attempt + 1
            self._write_status(
                resource,
                _status(
                    resource,
                    phase="Recovering",
                    attempt=next_attempt,
                    activeNodes=list(active),
                    spareNodes=list(spares),
                    clusterName=attempt_name(run.identity.name, next_attempt),
                    cleanupClusterName=cluster_name,
                    rayAddress="",
                    submissionId="",
                    resultWaitStartedAt="",
                    checkpoint=result.get("checkpoint"),
                    conditions=[
                        _condition(
                            "Ready",
                            "False",
                            "CheckpointStopCancelledAfterAcceptance",
                            "desired state is running; scheduling a new attempt",
                        )
                    ],
                ),
            )
            return "Recovering"
        return self._recover(resource, run, current, result, active, spares, cluster_name)

    def _recover(
        self,
        resource: Mapping[str, Any],
        run: Run,
        current: Mapping[str, Any],
        result: Mapping[str, Any],
        active: tuple[str, ...],
        spares: tuple[str, ...],
        cluster_name: str,
    ) -> str:
        raw_scope = str(result.get("failureScope", "unknown"))
        try:
            scope = FailureScope(raw_scope)
        except ValueError:
            scope = FailureScope.UNKNOWN
        reported_failed = tuple(
            sorted({item for item in result.get("failedNodes", []) if isinstance(item, str) and item in active})
        )
        failed = reported_failed
        diagnosis_stable = False
        survivors_ready = False
        healthy_spares: list[str] = []
        diagnosis = dict(current.get("diagnosis")) if isinstance(current.get("diagnosis"), Mapping) else {}
        if scope is FailureScope.HARDWARE and self.health is None:
            # A runtime-local hardware suspicion is not authoritative without
            # device health evidence. Retry it as infrastructure on the same topology.
            scope = FailureScope.INFRASTRUCTURE
        if scope is FailureScope.HARDWARE:
            observation = self.health.observe((*active, *spares))
            nodes = observation.get("nodes")
            if not isinstance(nodes, Mapping):
                raise RetryableControllerError("hardware health evidence is temporarily incomplete")

            def evidence_complete(node: str) -> bool:
                report = nodes.get(node)
                if not isinstance(report, Mapping):
                    return False
                explicit = report.get("complete")
                if isinstance(explicit, bool):
                    return explicit
                return report.get("hardwareHealthy") is not None

            if not all(evidence_complete(node) for node in active):
                raise RetryableControllerError("active-node health evidence is temporarily incomplete")
            observed_failed = {
                node for node in active if nodes.get(node, {}).get("hardwareHealthy") is False
            }
            failed = tuple(sorted(observed_failed))
            streak = (
                int(diagnosis.get("consecutive", 0)) + 1
                if tuple(diagnosis.get("failedNodes", ())) == failed
                else 1
            )
            healthy_spares = [
                node
                for node in spares
                if evidence_complete(node)
                and nodes.get(node, {}).get("hardwareHealthy") is True
                and nodes.get(node, {}).get("idle") is True
            ]
            diagnosis = {
                "failedNodes": list(failed),
                "reportedFailedNodes": list(reported_failed),
                "consecutive": streak,
                "healthySpareNodes": healthy_spares,
                "observedAt": utc_now(),
            }
            if streak < self.stable_diagnosis_samples:
                self._write_status(resource, _status(resource, phase="Running", diagnosis=diagnosis))
                return "Running"
            if failed:
                diagnosis_stable = True
                remaining_replacements = run.max_replacements - int(current.get("replacementsUsed", 0))
                if (
                    remaining_replacements >= len(failed)
                    and len(healthy_spares) < len(failed)
                    and any(not evidence_complete(node) for node in spares)
                ):
                    raise RetryableControllerError("spare-node health evidence is temporarily incomplete")
            else:
                # Runtime-local suspicion without provider confirmation is not
                # sufficient for replacement; consume a normal retry instead.
                scope = FailureScope.INFRASTRUCTURE
            survivors_ready = all(
                nodes.get(node, {}).get("hardwareHealthy") is True
                and nodes.get(node, {}).get("idle") is True
                for node in active
                if node not in failed
            )

        checkpoint_value = result.get("checkpointConsistent")
        checkpoint_evidence: bool | None
        if checkpoint_value is True:
            checkpoint_evidence = True
        elif checkpoint_value is False and (scope is FailureScope.CHECKPOINT or result.get("checkpointAvailable") is True):
            checkpoint_evidence = False
        else:
            # No committed checkpoint yet is not evidence of disagreement; retry may start cleanly.
            checkpoint_evidence = None
        policy = RecoveryPolicy(run.same_topology_retries, run.max_replacements)
        retries_used = int(current.get("retriesUsed", 0))
        replacements_used = int(current.get("replacementsUsed", 0))
        decision = decide_recovery(
            policy,
            RecoveryEvidence(
                scope=scope,
                failed_nodes=failed,
                diagnosis_stable=diagnosis_stable,
                checkpoint_consistent=checkpoint_evidence,
                survivors_healthy_and_idle=survivors_ready,
                spares_healthy_and_idle=len(healthy_spares),
            ),
            retries_used=retries_used,
            replacements_used=replacements_used,
        )
        if decision.action is RecoveryAction.MANUAL_REQUIRED:
            raise ControllerError(decision.reason)
        if decision.action is RecoveryAction.STOP:
            self._write_status(
                resource,
                _status(resource, phase="Stopped", completionTime=utc_now(), conditions=[
                    _condition("Ready", "False", "Stopped", decision.reason)
                ]),
            )
            return "Stopped"

        next_active = list(active)
        next_spares = list(spares)
        if decision.action is RecoveryAction.REPLACE_NODES:
            available = [node for node in next_spares if node in healthy_spares]
            for bad in failed:
                replacement = available.pop(0)
                next_spares.remove(replacement)
                next_active[next_active.index(bad)] = replacement
            replacements_used += len(failed)
            retries_used = 0
        else:
            retries_used += 1

        next_attempt = int(current.get("attempt", 0)) + 1
        self._write_status(
            resource,
            _status(
                resource,
                phase="Recovering",
                attempt=next_attempt,
                activeNodes=next_active,
                spareNodes=next_spares,
                clusterName=attempt_name(run.identity.name, next_attempt),
                cleanupClusterName=cluster_name,
                rayAddress="",
                submissionId="",
                resultWaitStartedAt="",
                retriesUsed=retries_used,
                replacementsUsed=replacements_used,
                diagnosis=diagnosis,
                conditions=[_condition("Ready", "False", "RecoveryScheduled", decision.reason)],
            ),
        )
        return "Recovering"


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default=os.environ.get("POD_NAMESPACE", "kcc-training"))
    parser.add_argument("--runtime-service-account", default="kcc-training-runtime")
    parser.add_argument("--identity", default=os.environ.get("POD_NAME", socket.gethostname()))
    parser.add_argument("--poll-seconds", type=float, default=10)
    parser.add_argument(
        "--start-timeout-seconds",
        type=int,
        default=int(os.environ.get("KCC_START_TIMEOUT_SECONDS", "900")),
    )
    parser.add_argument(
        "--result-timeout-seconds",
        type=int,
        default=int(os.environ.get("KCC_RESULT_TIMEOUT_SECONDS", "120")),
    )
    parser.add_argument(
        "--lease-duration-seconds",
        type=int,
        default=int(os.environ.get("KCC_LEASE_DURATION_SECONDS", "30")),
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    reconciler_factory: Callable[..., Reconciler] | None = None,
    jobs_factory: Callable[[], RayJobsRest] | None = None,
    profile_loader: Callable[[Mapping[str, Any]], RuntimeProfile] | None = None,
    recipe_loader: Callable[[Mapping[str, Any]], Recipe] | None = None,
    renderer: Callable[..., tuple[Mapping[str, Any], Mapping[str, Any]]] | None = None,
    result_validator: Callable[[Mapping[str, Any] | None, Run, int], Mapping[str, Any] | None] | None = None,
) -> int:
    args = make_parser().parse_args(argv)
    api = KubernetesApi()
    lock = LeaseLock(
        api,
        namespace=args.namespace,
        name="kcc-training-controller",
        identity=args.identity,
        duration_seconds=max(15, args.lease_duration_seconds),
    )
    factory = reconciler_factory or Reconciler
    dependencies: dict[str, Any] = {}
    if profile_loader is not None:
        dependencies["profile_loader"] = profile_loader
    if recipe_loader is not None:
        dependencies["recipe_loader"] = recipe_loader
    if renderer is not None:
        dependencies["renderer"] = renderer
    if result_validator is not None:
        dependencies["result_validator"] = result_validator
    reconciler = factory(
        api,
        (jobs_factory or RayJobsRest)(),
        runtime_service_account=args.runtime_service_account,
        start_timeout_seconds=args.start_timeout_seconds,
        result_timeout_seconds=args.result_timeout_seconds,
        **dependencies,
    )
    collection = namespaced_path(GROUP, VERSION, args.namespace, "trainingruns")
    while True:
        try:
            if lock.acquire_or_renew():
                with lock.maintain():
                    for resource in api.list(collection).get("items", []):
                        if not lock.held:
                            break
                        if isinstance(resource, Mapping):
                            reconciler.reconcile(resource)
        except Exception as error:
            print(f"controller loop failed: {error}", file=sys.stderr, flush=True)
        time.sleep(max(1.0, args.poll_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
