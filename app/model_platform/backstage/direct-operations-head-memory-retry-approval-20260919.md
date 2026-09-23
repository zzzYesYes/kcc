# Direct Operations head-memory retry approval request — 2026-09-19

> Status: approved, executed once, and completed successfully on 2026-09-19.
> The inference resources were stopped and cleaned, the Running Window was
> closed, and the Operator's exact Volcano-enabled spec was restored. This
> approval is consumed and must not be reused for another Start.

## Decision requested

Approve one more bounded Direct Operations production acceptance for
`model-serving/qwen38-27b`, using the same temporary KubeRay default-scheduler
window that correctly preserved the approved physical allocation 8/9.

The prior approved retry conclusively passed the scheduler boundary:

- capacity selected `Ascend910-8,Ascend910-9` from an idle A3;
- the worker used `default-scheduler` with the complete A3 nodeSelector;
- `huawei.com/Ascend910` and `huawei.com/AscendReal` were both 8/9;
- the container exposed only `/dev/davinci8` and `/dev/davinci9`;
- no Volcano PodGroup was created.

It failed later because the TP2 RuntimeProfile omitted head resources and
Direct Operations rendered the generic `8Gi` head fallback. The Ray head was
OOMKilled before Serve became ready. Gitea PR
`gitadmin/model-platform-config#62` is now merged to `main` and explicitly
sets `headCPU: "2"`, `headMemory: 16Gi`, `workerCPU: "48"`, and
`workerMemory: 256Gi`, matching the existing reviewed production baseline.
The catalog/schema validators and six unit tests pass, and the merged main
branch contains the four values.

## Current safe baseline

- `qwen38-27b` is Stopped at generation 97 with zero workers.
- The Running Window is closed.
- No Qwen RayService, Pod, PodGroup or PVC remains; cache PV data is retained.
- The KubeRay Operator spec is byte-for-byte equivalent at `.spec` to the
  captured pre-test version and again includes `--batch-scheduler=volcano`.
- `ds/dsv4-tp8`, the K12 RayCluster and `ray-demo/raycluster-npu-demo` remain
  Ready with the same Pod UIDs and restart counts as the baseline.
- The final capacity check reports no host process in the configured pool and
  again selects 8/9.

## Guarded retry after approval

1. Reconfirm all baseline assertions and that Gitea `main` still exposes the
   explicit `16Gi` head contract.
2. Temporarily remove only the Operator's exact Volcano argument with an
   optimistic-lock JSON Patch and wait for `1/1 Ready`.
3. Reclaim only the named Retain cache PV's stale claimRef after the same
   phase/path/node/policy/resourceVersion checks; run a fresh capacity check.
4. Open the Running Window and issue exactly one Start from saved config v2.
   Before model acceptance, require rendered head memory `16Gi`, worker
   scheduler `default-scheduler`, both device annotations 8/9, and only
   `/dev/davinci8` plus `/dev/davinci9` inside the worker.
5. Require stable head/worker restart counts, RayService/Serve readiness,
   EndpointSlice readiness, `/v1/models`, and one real chat completion.
6. Direct Stop, close the window, remove only orphaned completed cache objects,
   retain cache data, restore the exact Volcano argument, and re-verify the
   other RayClusters and zero A3 processes.

Any new mismatch, OOM, restart loop or timeout ends this single retry. Restore
the Operator and clean the inference resources first, then preserve evidence
and request another decision rather than issuing another Start.
