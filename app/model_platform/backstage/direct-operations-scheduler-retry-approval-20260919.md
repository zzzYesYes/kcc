# Direct Operations scheduler retry approval request — 2026-09-19

> Status: approved and consumed once on 2026-09-19. The scheduler/device
> assertion passed, but the acceptance stopped at a separate Ray head memory
> failure. Do not reuse this approval for another Start; see
> `direct-operations-head-memory-retry-approval-20260919.md`.

## Decision requested

The 2026-09-19 production acceptance stopped safely when the approved static
allocation `Ascend910-8,Ascend910-9` became actual worker annotations
`Ascend910-2,Ascend910-3` under Volcano/MindX. The worker carried the required
A3 `nodeSelector`; adding it again is not a fix. The Running Window is closed,
`qwen38-27b` is Stopped, and no serving Pod or RayService remains.

Request a separate, bounded approval to temporarily remove
`--batch-scheduler=volcano` from the single KubeRay Operator Deployment during
one inference acceptance window, then restore the flag. This follows the
previously successful default-scheduler/static-8/9 path recorded in
`qwen38-ray-tp2-execution-20260819.md` and the decision boundary in
`tekton/running-gate-and-first-controlled-start-20260828.md`. KubeRay's
upstream integration documents `--batch-scheduler=volcano` as an operator-level
setting, not a per-Qwen selector:
https://docs.ray.io/en/master/cluster/kubernetes/k8s-ecosystem/volcano.html

This is **not approved or applied** by this record. It affects the global
Operator, currently Helm-managed in namespace `ray-mangement`, and therefore
potentially affects three other live RayClusters: `ds/dsv4-tp8`,
`k12/k12-platform-cpu-k12-clean-qa-pipeline-k12-clean-qa`, and
`ray-demo/raycluster-npu-demo`. Their Pods and RayCluster health must be
snapshotted before any operator change, and there must be no concurrent Helm
reconciliation or Ray workload upgrade. The durable Helm release values must
be backed up and reconciled with the temporary change; otherwise a future Helm
upgrade could restore Volcano unexpectedly.

## Read-only candidate verification already completed

- Live KubeRay Operator: Helm chart `kuberay-operator-1.6.0`, one Ready replica,
  Recreate strategy, revision 7. Its first container argument is exactly
  `--batch-scheduler=volcano`.
- A server-side JSON Patch dry-run with a test on that exact argument and a
  remove operation succeeded. The candidate contains all remaining arguments
  unchanged. No deployment generation or Pod was changed by the dry-run.
- Backstage revision 72 stays Ready on immutable image digest
  `sha256:dcc269a6156d9b02aa32d8f9a9e0aefa11996abe742038d44335ea401cdeb898`.
  The worker-replica and Stop-during-Start code fixes are in Gitea PR #17.

## Proposed guarded sequence after approval

1. Reconfirm `qwen38-27b` Stopped, Running Window false, no model-serving
   RayService/Pod, and A3 exporter plus Kubernetes allocations idle. Capture
   the three other RayClusters' Pod UIDs, readiness and restart counts, the
   Operator Deployment JSON/Helm values, and the exact rollback argument.
2. Apply an optimistic-lock JSON Patch to remove only the first Operator
   argument, wait for one Ready replica, and verify the operator no longer
   sets Volcano scheduler/PodGroup on a Qwen candidate. Do not edit Volcano,
   device-plugin, A3 host processes, or other RayClusters.
3. Reclaim only the named Retain cache PV's stale old PVC claimRef if it is
   Released, using the reviewed resourceVersion/path/node/policy checks.
4. Recheck all 16 exporter samples and the target 8/9 pair; open the Running
   Window; Direct Start saved TP=2 configuration v2 once. Assert XR worker=1,
   Pod scheduled on A3, and actual `Ascend910`, `AscendReal`, container-visible
   device IDs equal 8/9 before treating the model as accepted. On any mismatch
   or timeout, Direct Stop and close the window immediately.
5. If devices match, complete Ray/Serve/vLLM readiness, EndpointSlice,
   `/v1/models`, and one real chat request. Then Direct Stop, close the window,
   wait for zero model-serving Ray/Pod/PVC objects, retain the cache PV data,
   and verify A3 process counts returned to zero.
6. Restore the exact `--batch-scheduler=volcano` Operator argument, wait for
   readiness, compare the other RayClusters with the baseline, and verify
   Helm/live configuration is consistent. Only then mark the acceptance
   complete. If any step fails, rollback the Operator first if necessary,
   preserve failure evidence, and request another review rather than retrying
   Start automatically.

Long-term production operation still needs a reviewed automatic post-schedule
device-consistency guard and/or a verified Volcano/MindX allocation repair.
The temporary override is solely a one-time acceptance path, not a permanent
claim that the current Volcano static-allocation contract is correct.
