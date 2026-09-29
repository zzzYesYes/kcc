import json
import unittest

from kcc_training.controller import Reconciler
from kcc_training.kube_api import KubernetesApiError, core_namespaced_path, namespaced_path
from tests.test_api_v1beta1 import IMAGE, resource


def resources():
    profile = resource("TrainingRuntimeProfile", "a3", {
        "images": {"head": IMAGE, "worker": IMAGE}, "rayVersion": "2.49.0",
        "accelerator": {"resourceName": "huawei.com/Ascend910", "devicesPerNode": 8, "runtimeClassName": "ascend"},
        "workspace": {"claimName": "workspace", "mountPath": "/workspace"},
        "scheduling": {"activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"], "headSelector": {"arch": "amd64"}, "workerSelector": {"npu": "a3"}},
        "integrations": {"rankTableProvider": "clusterd", "healthProvider": "npu-exporter"},
    })
    recipe = resource("TrainingRecipe", "recipe", {
        "framework": "mindspeed", "command": ["python", "pretrain.py"], "workingDirectory": ".", "environment": {},
        "artifacts": {"source": "artifact://training/source/v1", "model": "artifact://training/model/v1", "data": "artifact://training/data/v1", "outputSubpath": "runs/x"},
    })
    run = resource("TrainingRun", "run-1", {
        "runtimeProfile": "a3", "recipe": "recipe", "workers": 2,
        "recovery": {"sameTopologyRetries": 2, "maxReplacements": 1, "noProgressSeconds": 3600},
    })
    return run, profile, recipe


class FakeApi:
    def __init__(self, run, profile, recipe):
        ns = "training"
        self.objects = {
            namespaced_path("training.kcc.io", "v1beta1", ns, "trainingruntimeprofiles", "a3"): profile,
            namespaced_path("training.kcc.io", "v1beta1", ns, "trainingrecipes", "recipe"): recipe,
        }
        self.statuses = []
        self.upserts = []
        self.deletes = []
        self.delete_immediately = True
        self.run = run

    def get(self, path):
        return self.objects.get(path)

    def upsert(self, collection, item, desired):
        self.upserts.append((collection, item, desired))
        stored = json.loads(json.dumps(desired))
        stored.setdefault("metadata", {})["resourceVersion"] = "1"
        stored["metadata"].setdefault("uid", "created-uid")
        self.objects[item] = stored
        return stored

    def update_status(self, path, current, status):
        del path, current
        self.statuses.append(dict(status))
        updated = json.loads(json.dumps(self.run))
        updated["status"] = dict(status)
        return updated

    def delete_owned(self, path, uid):
        self.deletes.append((path, uid))
        if self.delete_immediately:
            self.objects.pop(path, None)


class FakeJobs:
    def __init__(self):
        self.job_status = "RUNNING"
        self.submissions = []
        self.stops = []
        self.status_error = None

    def submit_once(self, address, submission, command, metadata):
        self.submissions.append((address, submission, tuple(command), metadata))
        return submission

    def stop(self, address, submission):
        self.stops.append((address, submission))

    def status(self, address, submission):
        if self.status_error is not None:
            raise self.status_error
        del address, submission
        return self.job_status


def with_status(run, status):
    value = json.loads(json.dumps(run))
    value["status"] = status
    value["metadata"]["resourceVersion"] = str(int(value["metadata"]["resourceVersion"]) + 1)
    return value


class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.run, profile, recipe = resources()
        self.api = FakeApi(self.run, profile, recipe)
        self.jobs = FakeJobs()
        self.reconciler = Reconciler(self.api, self.jobs, runtime_service_account="runtime")

    def test_pending_provisions_attempt_with_shared_workspace(self):
        self.assertEqual(self.reconciler.reconcile(self.run), "Starting")
        self.assertEqual(self.api.statuses[-1]["phase"], "Starting")
        rendered = str(self.api.upserts)
        self.assertIn("persistentVolumeClaim", rendered)
        self.assertIn("/usr/local/Ascend/driver", rendered)

    def test_dependency_waits_for_success_and_cluster_release(self):
        self.run["spec"]["dependsOn"] = "run-0"
        dependency_path = namespaced_path(
            "training.kcc.io",
            "v1beta1",
            "training",
            "trainingruns",
            "run-0",
        )

        self.assertEqual(self.reconciler.reconcile(self.run), "Queued")
        self.assertEqual(self.api.statuses[-1]["phase"], "Queued")
        self.assertEqual(
            self.api.statuses[-1]["conditions"][0]["reason"],
            "WaitingForDependency",
        )
        self.assertEqual(self.api.upserts, [])

        dependency = json.loads(json.dumps(self.run))
        dependency["metadata"]["name"] = "run-0"
        dependency["status"] = {
            "phase": "Succeeded",
            "clusterName": "run-0-a00",
        }
        self.api.objects[dependency_path] = dependency
        dependency_cluster_path = namespaced_path(
            "ray.io",
            "v1",
            "training",
            "rayclusters",
            "run-0-a00",
        )
        self.api.objects[dependency_cluster_path] = {"metadata": {"name": "run-0-a00"}}

        queued = with_status(self.run, self.api.statuses[-1])
        self.assertEqual(self.reconciler.reconcile(queued), "Queued")
        self.assertEqual(
            self.api.statuses[-1]["conditions"][0]["reason"],
            "WaitingForDependencyCleanup",
        )
        self.assertEqual(self.api.upserts, [])

        del self.api.objects[dependency_cluster_path]
        released = with_status(self.run, self.api.statuses[-1])
        self.assertEqual(self.reconciler.reconcile(released), "Starting")
        self.assertEqual(self.api.statuses[-1]["phase"], "Starting")
        self.assertTrue(self.api.upserts)

    def test_suspended_dependent_run_does_not_wait_in_queue(self):
        self.run["spec"]["dependsOn"] = "run-0"
        self.run["spec"]["suspend"] = True
        self.assertEqual(self.reconciler.reconcile(self.run), "Suspended")
        self.assertEqual(self.api.statuses[-1]["phase"], "Suspended")
        self.assertEqual(self.api.upserts, [])

    def test_running_dependent_run_does_not_return_to_queue(self):
        self.run["spec"]["dependsOn"] = "run-0"
        running = with_status(
            self.run,
            {
                "phase": "Running",
                "observedGeneration": 1,
                "attempt": 0,
                "activeNodes": ["node-a", "node-b"],
                "spareNodes": ["node-c"],
                "clusterName": "run-1-a00",
                "rayAddress": "http://ray",
                "submissionId": "run-1-a00",
            },
        )
        cluster_path = namespaced_path(
            "ray.io",
            "v1",
            "training",
            "rayclusters",
            "run-1-a00",
        )
        self.api.objects[cluster_path] = {
            "metadata": {
                "name": "run-1-a00",
                "uid": "cluster-uid",
                "annotations": {"training.kcc.io/run-uid": "uid-1"},
            },
            "status": {"state": "ready", "readyWorkerReplicas": 2},
        }

        self.assertEqual(self.reconciler.reconcile(running), "Running")
        self.assertEqual(self.api.statuses, [])

    def test_ready_cluster_submits_effective_command_and_moves_running(self):
        self.run["spec"]["training"] = {
            "arguments": ["--micro-batch-size", "2"]
        }
        pending = self.reconciler.reconcile(self.run)
        self.assertEqual(pending, "Starting")
        status = self.api.statuses[-1]
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path]["status"] = {"state": "ready", "readyWorkerReplicas": 2}
        current = with_status(self.run, status)
        self.assertEqual(self.reconciler.reconcile(current), "Running")
        self.assertEqual(len(self.jobs.submissions), 1)
        self.assertEqual(
            self.jobs.submissions[0][2],
            ("python", "pretrain.py", "--micro-batch-size", "2"),
        )

    def test_success_requires_owned_result_configmap(self):
        status = {
            "phase": "Running", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {"metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}}}
        result_path = core_namespaced_path("training", "configmaps", "run-1-a00-result")
        result = {"schemaVersion": "kcc-runtime-result/v1", "runUid": "uid-1", "attempt": 0, "status": "PASS", "checkpointConsistent": True}
        self.api.objects[result_path] = {"metadata": {"annotations": {"training.kcc.io/run-uid": "uid-1", "training.kcc.io/attempt": "0"}}, "data": {"result.json": json.dumps(result)}}
        self.jobs.job_status = "SUCCEEDED"
        self.assertEqual(self.reconciler.reconcile(with_status(self.run, status)), "Succeeded")

    def test_terminal_job_waits_for_result_without_blocking_run(self):
        status = {
            "phase": "Running", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}},
            "status": {"state": "ready", "readyWorkerReplicas": 2},
        }
        self.jobs.job_status = "FAILED"
        self.assertEqual(self.reconciler.reconcile(with_status(self.run, status)), "Running")
        self.assertEqual(self.api.statuses[-1]["conditions"][0]["reason"], "RuntimeResultPending")

    def test_running_mirrors_owned_runtime_progress(self):
        status = {
            "phase": "Running", "attempt": 0,
            "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path(
            "ray.io", "v1", "training", "rayclusters", "run-1-a00"
        )
        self.api.objects[cluster_path] = {
            "metadata": {
                "uid": "cluster-uid",
                "annotations": {"training.kcc.io/run-uid": "uid-1"},
            }
        }
        progress = {
            "schemaVersion": "kcc-runtime-progress/v1",
            "runName": "run-1",
            "runUid": "uid-1",
            "attempt": 0,
            "stage": "HcclTest",
            "status": "Running",
            "message": "running HCCL AllReduce",
            "updatedAt": "2026-08-30T12:00:00Z",
            "details": {"expectedRanks": 16},
        }
        progress_path = core_namespaced_path(
            "training", "configmaps", "run-1-a00-progress"
        )
        self.api.objects[progress_path] = {
            "metadata": {
                "annotations": {
                    "training.kcc.io/run-uid": "uid-1",
                    "training.kcc.io/attempt": "0",
                }
            },
            "data": {"progress.json": json.dumps(progress)},
        }
        self.assertEqual(
            self.reconciler.reconcile(with_status(self.run, status)), "Running"
        )
        self.assertEqual(self.api.statuses[-1]["progress"], progress)
        self.assertEqual(
            self.api.statuses[-1]["conditions"][0]["reason"], "RuntimeProgress"
        )


    def test_suspend_after_checkpoint_waits_for_verified_runtime_result(self):
        status = {
            "phase": "Running",
            "attempt": 0,
            "activeNodes": ["node-a", "node-b"],
            "spareNodes": ["node-c"],
            "clusterName": "run-1-a00",
            "retriesUsed": 0,
            "replacementsUsed": 0,
            "rayAddress": "http://ray",
            "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path(
            "ray.io", "v1", "training", "rayclusters", "run-1-a00"
        )
        self.api.objects[cluster_path] = {
            "metadata": {
                "uid": "cluster-uid",
                "annotations": {"training.kcc.io/run-uid": "uid-1"},
            }
        }
        graceful = json.loads(json.dumps(self.run))
        graceful["metadata"]["generation"] = 2
        graceful["spec"]["suspend"] = True
        graceful["spec"]["suspendMode"] = "AfterCheckpoint"

        self.assertEqual(
            self.reconciler.reconcile(with_status(graceful, status)),
            "Stopping",
        )
        requested = self.api.statuses[-1]
        self.assertEqual(requested["phase"], "Stopping")
        self.assertEqual(requested["stopRequestGeneration"], 2)
        self.assertFalse(self.jobs.stops)
        self.assertFalse(self.api.deletes)

        control_path = core_namespaced_path(
            "training", "configmaps", "run-1-a00-control"
        )
        control = json.loads(
            self.api.objects[control_path]["data"]["control.json"]
        )
        self.assertEqual(control["action"], "StopAfterCheckpoint")
        self.assertEqual(control["requestGeneration"], 2)

        result_path = core_namespaced_path(
            "training", "configmaps", "run-1-a00-result"
        )
        result = {
            "schemaVersion": "kcc-runtime-result/v1",
            "runUid": "uid-1",
            "attempt": 0,
            "status": "STOPPED",
            "checkpointConsistent": True,
            "checkpointAvailable": True,
            "checkpoint": {"available": True, "iteration": 8},
            "failureScope": None,
            "failedNodes": [],
            "stopReason": "AfterCheckpoint",
            "stopRequestGeneration": 2,
            "stopBaselineIteration": 7,
        }
        self.api.objects[result_path] = {
            "metadata": {
                "annotations": {
                    "training.kcc.io/run-uid": "uid-1",
                    "training.kcc.io/attempt": "0",
                }
            },
            "data": {"result.json": json.dumps(result)},
        }
        self.assertEqual(
            self.reconciler.reconcile(with_status(graceful, requested)),
            "Suspended",
        )
        suspended = self.api.statuses[-1]
        self.assertEqual(suspended["phase"], "Suspended")
        self.assertEqual(suspended["checkpoint"]["iteration"], 8)
        self.assertEqual(suspended["stopBaselineIteration"], 7)
        self.assertEqual(len(self.api.deletes), 1)

    def test_cancelled_checkpoint_stop_restarts_if_runtime_already_stopped(self):
        status = {
            "phase": "Running",
            "attempt": 0,
            "activeNodes": ["node-a", "node-b"],
            "spareNodes": ["node-c"],
            "clusterName": "run-1-a00",
            "retriesUsed": 0,
            "replacementsUsed": 0,
            "rayAddress": "http://ray",
            "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path(
            "ray.io", "v1", "training", "rayclusters", "run-1-a00"
        )
        self.api.objects[cluster_path] = {
            "metadata": {
                "uid": "cluster-uid",
                "annotations": {"training.kcc.io/run-uid": "uid-1"},
            }
        }
        result_path = core_namespaced_path(
            "training", "configmaps", "run-1-a00-result"
        )
        result = {
            "schemaVersion": "kcc-runtime-result/v1",
            "runUid": "uid-1",
            "attempt": 0,
            "status": "STOPPED",
            "checkpointConsistent": True,
            "checkpointAvailable": True,
            "checkpoint": {"available": True, "iteration": 8},
            "failureScope": None,
            "failedNodes": [],
            "stopReason": "AfterCheckpoint",
            "stopRequestGeneration": 2,
            "stopBaselineIteration": 7,
        }
        self.api.objects[result_path] = {
            "metadata": {
                "annotations": {
                    "training.kcc.io/run-uid": "uid-1",
                    "training.kcc.io/attempt": "0",
                }
            },
            "data": {"result.json": json.dumps(result)},
        }
        self.assertEqual(
            self.reconciler.reconcile(with_status(self.run, status)),
            "Recovering",
        )
        recovery = self.api.statuses[-1]
        self.assertEqual(recovery["attempt"], 1)
        self.assertEqual(recovery["retriesUsed"], 0)
        self.assertEqual(recovery["checkpoint"]["iteration"], 8)

    def test_suspend_stops_and_deletes_then_resumes_with_new_attempt(self):
        status = {
            "phase": "Running", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}}
        }
        suspended = json.loads(json.dumps(self.run))
        suspended["metadata"]["generation"] = 2
        suspended["spec"]["suspend"] = True
        self.assertEqual(self.reconciler.reconcile(with_status(suspended, status)), "Stopping")
        stopping_status = self.api.statuses[-1]
        control_path = core_namespaced_path(
            "training", "configmaps", "run-1-a00-control"
        )
        control = json.loads(self.api.objects[control_path]["data"]["control.json"])
        self.assertEqual(control["action"], "StopImmediate")
        self.assertFalse(self.jobs.stops)
        self.assertFalse(self.api.deletes)

        result_path = core_namespaced_path(
            "training", "configmaps", "run-1-a00-result"
        )
        result = {
            "schemaVersion": "kcc-runtime-result/v1",
            "runUid": "uid-1",
            "attempt": 0,
            "status": "STOPPED",
            "checkpointConsistent": True,
            "checkpointAvailable": False,
            "checkpoint": None,
            "failureScope": None,
            "failedNodes": [],
            "stopReason": "Immediate",
            "stopRequestGeneration": 2,
            "checkpointCleanup": {
                "completed": True,
                "removedEntries": 3,
                "retainedIteration": None,
            },
        }
        self.api.objects[result_path] = {
            "metadata": {
                "annotations": {
                    "training.kcc.io/run-uid": "uid-1",
                    "training.kcc.io/attempt": "0",
                }
            },
            "data": {"result.json": json.dumps(result)},
        }
        self.assertEqual(
            self.reconciler.reconcile(with_status(suspended, stopping_status)),
            "Suspended",
        )
        suspended_status = self.api.statuses[-1]
        self.assertEqual(len(self.api.deletes), 1)

        self.assertEqual(
            self.reconciler.reconcile(with_status(self.run, suspended_status)),
            "Recovering",
        )
        self.assertEqual(self.api.statuses[-1]["attempt"], 1)

    def test_suspend_during_recovery_cleans_old_cluster_without_skipping_attempt(self):
        status = {
            "phase": "Recovering", "attempt": 1,
            "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a01", "cleanupClusterName": "run-1-a00",
            "retriesUsed": 1, "replacementsUsed": 0,
        }
        old_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[old_path] = {
            "metadata": {"uid": "old-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}}
        }
        suspended = json.loads(json.dumps(self.run))
        suspended["spec"]["suspend"] = True
        self.assertEqual(self.reconciler.reconcile(with_status(suspended, status)), "Suspended")
        suspended_status = self.api.statuses[-1]
        self.assertEqual(self.api.deletes[0][0], old_path)

        self.assertEqual(self.reconciler.reconcile(with_status(self.run, suspended_status)), "Recovering")
        self.assertEqual(self.api.statuses[-1]["attempt"], 1)
        self.assertEqual(self.api.statuses[-1]["clusterName"], "run-1-a01")

    def test_recovery_persists_intent_before_deleting_and_waits_for_absence(self):
        status = {
            "phase": "Running", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}},
            "status": {"state": "ready", "readyWorkerReplicas": 2},
        }
        result_path = core_namespaced_path("training", "configmaps", "run-1-a00-result")
        result = {
            "schemaVersion": "kcc-runtime-result/v1", "runUid": "uid-1", "attempt": 0,
            "status": "FAIL", "checkpointConsistent": False, "failureScope": "software", "failedNodes": [],
        }
        self.api.objects[result_path] = {
            "metadata": {"annotations": {"training.kcc.io/run-uid": "uid-1", "training.kcc.io/attempt": "0"}},
            "data": {"result.json": json.dumps(result)},
        }
        self.jobs.job_status = "FAILED"
        self.api.delete_immediately = False
        self.assertEqual(self.reconciler.reconcile(with_status(self.run, status)), "Recovering")
        recovery_status = self.api.statuses[-1]
        self.assertFalse(self.api.deletes)
        self.assertEqual(recovery_status["cleanupClusterName"], "run-1-a00")

        self.assertEqual(self.reconciler.reconcile(with_status(self.run, recovery_status)), "Recovering")
        self.assertEqual(len(self.api.deletes), 1)
        self.assertFalse(self.api.upserts)

        self.api.objects.pop(cluster_path)
        self.assertEqual(self.reconciler.reconcile(with_status(self.run, recovery_status)), "Starting")
        self.assertTrue(self.api.upserts)

    def test_starting_attempt_is_recreated_when_cluster_is_missing(self):
        status = {
            "phase": "Starting", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
        }
        self.assertEqual(self.reconciler.reconcile(with_status(self.run, status)), "Starting")
        self.assertEqual(self.api.statuses[-1]["clusterName"], "run-1-a00")
        self.assertTrue(self.api.upserts)

    def test_start_timeout_uses_recovery_budget(self):
        status = {
            "phase": "Starting", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
            "startTime": "1970-01-01T00:00:00Z",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}},
            "status": {"state": "initializing", "readyWorkerReplicas": 0},
        }
        reconciler = Reconciler(
            self.api, self.jobs, runtime_service_account="runtime", start_timeout_seconds=10, clock=lambda: 100
        )
        self.assertEqual(reconciler.reconcile(with_status(self.run, status)), "Recovering")
        self.assertEqual(self.api.statuses[-1]["attempt"], 1)
        self.assertFalse(self.api.deletes)

    def test_successful_terminal_state_releases_cluster_on_next_poll(self):
        status = {
            "phase": "Succeeded", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}}
        }
        self.assertEqual(self.reconciler.reconcile(with_status(self.run, status)), "Succeeded")
        self.assertEqual(len(self.api.deletes), 1)

    def test_owned_result_wins_when_dashboard_history_is_missing(self):
        status = {
            "phase": "Running", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}},
            "status": {"state": "ready", "readyWorkerReplicas": 2},
        }
        result = {
            "schemaVersion": "kcc-runtime-result/v1", "runUid": "uid-1", "attempt": 0,
            "status": "PASS", "checkpointConsistent": True,
        }
        self.api.objects[core_namespaced_path("training", "configmaps", "run-1-a00-result")] = {
            "metadata": {"annotations": {"training.kcc.io/run-uid": "uid-1", "training.kcc.io/attempt": "0"}},
            "data": {"result.json": json.dumps(result)},
        }
        self.jobs.job_status = "NOT_FOUND"
        self.assertEqual(self.reconciler.reconcile(with_status(self.run, status)), "Succeeded")
        self.assertFalse(self.jobs.submissions)

    def test_dashboard_not_found_without_result_uses_new_attempt(self):
        status = {
            "phase": "Running", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}},
            "status": {"state": "ready", "readyWorkerReplicas": 2},
        }
        self.jobs.job_status = "NOT_FOUND"
        self.assertEqual(self.reconciler.reconcile(with_status(self.run, status)), "Recovering")
        self.assertEqual(self.api.statuses[-1]["attempt"], 1)
        self.assertFalse(self.jobs.submissions)

    def test_unconfirmed_runtime_hardware_suspicion_retries_without_replacement(self):
        status = {
            "phase": "Running", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}},
            "status": {"state": "ready", "readyWorkerReplicas": 2},
        }
        result = {
            "schemaVersion": "kcc-runtime-result/v1", "runUid": "uid-1", "attempt": 0,
            "status": "FAIL", "checkpointConsistent": True,
            "failureScope": "hardware", "failedNodes": ["node-a"],
        }
        self.api.objects[core_namespaced_path("training", "configmaps", "run-1-a00-result")] = {
            "metadata": {"annotations": {"training.kcc.io/run-uid": "uid-1", "training.kcc.io/attempt": "0"}},
            "data": {"result.json": json.dumps(result)},
        }
        self.jobs.job_status = "FAILED"

        class Healthy:
            def observe(self, nodes):
                return {"complete": True, "nodes": {
                    node: {"hardwareHealthy": True, "idle": True} for node in nodes
                }}

        reconciler = Reconciler(
            self.api, self.jobs, runtime_service_account="runtime", health=Healthy(), stable_diagnosis_samples=1
        )
        self.assertEqual(reconciler.reconcile(with_status(self.run, status)), "Recovering")
        self.assertEqual(self.api.statuses[-1]["activeNodes"], ["node-a", "node-b"])
        self.assertEqual(self.api.statuses[-1]["spareNodes"], ["node-c"])
        self.assertEqual(self.api.statuses[-1]["retriesUsed"], 1)

    def test_sampled_checkpoint_pass_is_preserved_in_status(self):
        status = {
            "phase": "Running", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}},
            "status": {"state": "ready", "readyWorkerReplicas": 2},
        }
        checkpoint = {
            "available": True,
            "iteration": 42,
            "hashMode": "sampled-sha256",
            "sampleBytesPerFile": 1048576,
            "snapshotSha256": "a" * 64,
            "trackerSha256": "b" * 64,
        }
        result = {
            "schemaVersion": "kcc-runtime-result/v1", "runUid": "uid-1", "attempt": 0,
            "status": "PASS", "checkpointAvailable": True,
            "checkpointConsistent": True, "checkpoint": checkpoint,
        }
        self.api.objects[core_namespaced_path("training", "configmaps", "run-1-a00-result")] = {
            "metadata": {"annotations": {"training.kcc.io/run-uid": "uid-1", "training.kcc.io/attempt": "0"}},
            "data": {"result.json": json.dumps(result)},
        }
        self.assertEqual(self.reconciler.reconcile(with_status(self.run, status)), "Succeeded")
        self.assertEqual(self.api.statuses[-1]["checkpoint"], checkpoint)

    def test_first_hardware_diagnosis_preserves_reported_failed_nodes(self):
        status = {
            "phase": "Running", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}},
            "status": {"state": "ready", "readyWorkerReplicas": 2},
        }
        result = {
            "schemaVersion": "kcc-runtime-result/v1", "runUid": "uid-1", "attempt": 0,
            "status": "FAIL", "checkpointConsistent": True,
            "failureScope": "hardware", "failedNodes": ["node-a"],
        }
        self.api.objects[core_namespaced_path("training", "configmaps", "run-1-a00-result")] = {
            "metadata": {"annotations": {"training.kcc.io/run-uid": "uid-1", "training.kcc.io/attempt": "0"}},
            "data": {"result.json": json.dumps(result)},
        }

        class Health:
            def observe(self, nodes):
                del nodes
                return {"complete": True, "nodes": {
                    "node-a": {"hardwareHealthy": False, "idle": True},
                    "node-b": {"hardwareHealthy": True, "idle": True},
                    "node-c": {"hardwareHealthy": True, "idle": True},
                }}

        reconciler = Reconciler(self.api, self.jobs, runtime_service_account="runtime", health=Health())
        self.assertEqual(reconciler.reconcile(with_status(self.run, status)), "Running")
        diagnosis = self.api.statuses[-1]["diagnosis"]
        self.assertEqual(diagnosis["reportedFailedNodes"], ["node-a"])
        self.assertEqual(diagnosis["failedNodes"], ["node-a"])

    def test_manual_required_releases_only_owned_cluster(self):
        status = {
            "phase": "ManualRequired", "attempt": 0, "clusterName": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}}
        }
        self.assertEqual(self.reconciler.reconcile(with_status(self.run, status)), "ManualRequired")
        self.assertEqual(len(self.api.deletes), 1)

    def test_transient_ray_error_keeps_running(self):
        from kcc_training.ray_jobs_rest import RayJobsRestError

        status = {
            "phase": "Running", "attempt": 0, "activeNodes": ["node-a", "node-b"], "spareNodes": ["node-c"],
            "clusterName": "run-1-a00", "retriesUsed": 0, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}},
            "status": {"state": "ready", "readyWorkerReplicas": 2},
        }
        self.jobs.status_error = RayJobsRestError(503, "dashboard unavailable")
        self.assertEqual(self.reconciler.reconcile(with_status(self.run, status)), "Running")
        self.assertEqual(self.api.statuses[-1]["conditions"][0]["reason"], "ReconcileRetrying")

    def test_referenced_profile_can_arrive_after_training_run(self):
        profile_path = namespaced_path(
            "training.kcc.io", "v1beta1", "training", "trainingruntimeprofiles", "a3"
        )
        self.api.objects.pop(profile_path)
        self.assertEqual(self.reconciler.reconcile(self.run), "Pending")
        self.assertEqual(self.api.statuses[-1]["conditions"][0]["reason"], "ReconcileRetrying")

    def test_transient_kubernetes_error_does_not_become_manual(self):
        original_get = self.api.get

        def unavailable(path):
            if "trainingruntimeprofiles" in path:
                raise KubernetesApiError(503, "apiserver unavailable")
            return original_get(path)

        self.api.get = unavailable
        self.assertEqual(self.reconciler.reconcile(self.run), "Pending")
        self.assertFalse(self.api.statuses)

    def test_stable_kubernetes_health_retries_hardware_suspicion_without_replacement(self):
        from kcc_training.controller_stable import StableReconciler

        profile_path = namespaced_path(
            "training.kcc.io", "v1beta1", "training", "trainingruntimeprofiles", "a3"
        )
        self.api.objects[profile_path]["spec"]["integrations"]["healthProvider"] = "kubernetes"
        run = json.loads(json.dumps(self.run))
        run["spec"]["recovery"]["maxReplacements"] = 0
        self.api.run = run
        status = {
            "phase": "Running", "attempt": 0, "activeNodes": ["node-a", "node-b"],
            "spareNodes": ["node-c"], "clusterName": "run-1-a00",
            "retriesUsed": 0, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}},
            "status": {"state": "ready", "readyWorkerReplicas": 2},
        }
        result = {
            "schemaVersion": "kcc-runtime-result/v1", "runUid": "uid-1", "attempt": 0,
            "status": "FAIL", "checkpointConsistent": True,
            "failureScope": "hardware", "failedNodes": ["node-a"],
        }
        self.api.objects[core_namespaced_path("training", "configmaps", "run-1-a00-result")] = {
            "metadata": {"annotations": {"training.kcc.io/run-uid": "uid-1", "training.kcc.io/attempt": "0"}},
            "data": {"result.json": json.dumps(result)},
        }
        reconciler = StableReconciler(self.api, self.jobs, runtime_service_account="runtime")
        self.assertEqual(reconciler.reconcile(with_status(run, status)), "Recovering")
        recovered = self.api.statuses[-1]
        self.assertEqual(recovered["activeNodes"], ["node-a", "node-b"])
        self.assertEqual(recovered["retriesUsed"], 1)
        self.assertEqual(recovered["replacementsUsed"], 0)

    def test_hardware_replacement_ignores_an_incomplete_unneeded_spare(self):
        profile_path = namespaced_path(
            "training.kcc.io", "v1beta1", "training", "trainingruntimeprofiles", "a3"
        )
        self.api.objects[profile_path]["spec"]["scheduling"]["spareNodes"] = ["node-c", "node-d"]
        status = {
            "phase": "Running", "attempt": 0, "activeNodes": ["node-a", "node-b"],
            "spareNodes": ["node-c", "node-d"], "clusterName": "run-1-a00",
            "retriesUsed": 2, "replacementsUsed": 0,
            "rayAddress": "http://ray", "submissionId": "run-1-a00",
        }
        cluster_path = namespaced_path("ray.io", "v1", "training", "rayclusters", "run-1-a00")
        self.api.objects[cluster_path] = {
            "metadata": {"uid": "cluster-uid", "annotations": {"training.kcc.io/run-uid": "uid-1"}},
            "status": {"state": "ready", "readyWorkerReplicas": 2},
        }
        result = {
            "schemaVersion": "kcc-runtime-result/v1", "runUid": "uid-1", "attempt": 0,
            "status": "FAIL", "checkpointConsistent": True,
            "failureScope": "hardware", "failedNodes": ["node-a"],
        }
        self.api.objects[core_namespaced_path("training", "configmaps", "run-1-a00-result")] = {
            "metadata": {"annotations": {"training.kcc.io/run-uid": "uid-1", "training.kcc.io/attempt": "0"}},
            "data": {"result.json": json.dumps(result)},
        }
        self.jobs.job_status = "FAILED"

        class Health:
            def observe(self, nodes):
                del nodes
                return {"complete": False, "nodes": {
                    "node-a": {"complete": True, "hardwareHealthy": False, "idle": True},
                    "node-b": {"complete": True, "hardwareHealthy": True, "idle": True},
                    "node-c": {"complete": False, "hardwareHealthy": None, "idle": False},
                    "node-d": {"complete": True, "hardwareHealthy": True, "idle": True},
                }}

        reconciler = Reconciler(
            self.api, self.jobs, runtime_service_account="runtime", health=Health(), stable_diagnosis_samples=1
        )
        self.assertEqual(reconciler.reconcile(with_status(self.run, status)), "Recovering")
        self.assertEqual(self.api.statuses[-1]["activeNodes"], ["node-d", "node-b"])
        self.assertEqual(self.api.statuses[-1]["spareNodes"], ["node-c"])

if __name__ == "__main__":
    unittest.main()
