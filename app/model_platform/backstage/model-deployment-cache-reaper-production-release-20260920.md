# ModelDeployment cache reaper production release — 2026-09-20

## Result

The stopped-model cache cleanup repair is deployed on production `server-00`.
It changes only the existing `model-serving/model-deployment-cache-reaper`
ConfigMap, Role and CronJob. It does not change the ModelDeployment,
Composition, Ray, NPU, PVC or PV declarations.

The CronJob is enabled once per minute. A candidate deletion is accepted only
when all of these conditions hold:

- `qwen38-27b` is `Stopped`, `Synced=True` and `Ready=True`;
- no labelled RayService or RayCluster remains;
- no labelled cache Job is active;
- the Pod is terminal, unowned and not already deleting;
- deployment and cache-revision labels match the current XR; and
- the Pod actually mounts the exact `qwen38-27b-cache` PVC.

Deletion uses the observed Pod UID as an API precondition. The ServiceAccount
can delete Pods but cannot delete PVCs or PVs. The Retain cache PV and its data
remain outside the reaper's write permission.

## Source and validation

- Production Gitea PR: `gitadmin/model-platform-config#63`.
- Candidate commit: `8a8267158eba457e5c5d1d3f9fc3dcfe783d871e`.
- Main merge commit: `7575c15f5a935eec5bf5c2d0b02946a9f19e1a13`.
- Three focused Python unit tests passed.
- The rendered ServiceAccount, Role, RoleBinding, ConfigMap and CronJob passed
  production Kubernetes server-side dry-run before apply.
- PR PipelineRun `model-platform-config-validation-lc9gn` succeeded.
- Main PipelineRun `model-platform-config-validation-ccqcl` completed
  successfully, including the Gitea commit-status reporter.

## Production acceptance

Before apply, the XR was `Stopped/Synced=True/Ready=True`; no labelled Qwen
Pod, Job, PVC, RayService or RayCluster existed. The three unrelated
RayClusters in `ds`, `k12` and `ray-demo` were Ready.

The first scheduled Job `model-deployment-cache-reaper-29831110` started at
`2026-09-20T01:10:00Z`, completed at `01:10:05Z`, and logged:

```text
cache_reaper=PASS deleted=none
```

Its Pod ran only on AMD64 `server-00`, used the existing immutable CI-tools
digest, requested no Ascend resource and completed with zero restarts. A later
scheduled run also completed successfully at `01:16:05Z`. After release, the
XR and all three unrelated RayClusters retained their baseline health and no
labelled Qwen runtime or PVC resource appeared.

## Rollback

If the reaper behaves unexpectedly, first set the CronJob to `suspend=true`.
Then restore the previous cache-reaper files from config commit `3e090b3` and
apply only that rendered cache-reaper bundle. This returns the prior suspended
`*/5` schedule and script/RBAC without changing model runtime or retained cache
data.

## First post-release end-to-end preflight

A user-authorized inference Start/Stop acceptance was attempted later on
2026-09-20, specifically to exercise the reaper against a real Stop lifecycle.
The independent capacity checker rejected the test before the Running Window
was opened: fresh exporter samples (about 2.5 seconds old) reported process
count `1` on every A3 device ID 0 through 15, including all configured pool
devices 8 through 15. The exporter associated none of these processes with a
Kubernetes namespace, Pod or container, and no Kubernetes Pod on
`a3-server-00` requested `huawei.com/Ascend910`; the occupancy is therefore
outside the platform-managed Qwen workload.

No Start request was issued. The Running Window remained closed, the XR stayed
at generation 103 in `Stopped/Synced=True/Ready=True`, no labelled Qwen
Pod/Job/PVC/Ray resource was created, and the Retain PV remained Released with
the same claim UID. The deployed reaper remains operational, but its real
orphan-deletion branch still requires the next idle-NPU Start/Stop acceptance.
