# Direct Operations production acceptance continuation — 2026-09-19

## Outcome

The NPU capacity gate returned `allowed=true` for the approved TP=2 request,
selecting `Ascend910-8,Ascend910-9`. The A3 exporter supplied fresh process
counts for all 16 IDs and reported zero host processes. No other business Pod
was on A3. The controlled Running Window was opened only for the two Start
attempts and is now closed.

The end-to-end inference acceptance did **not** complete. The second Start
reached a Running Volcano PodGroup with one head and one worker on A3, but the
worker Pod's actual `huawei.com/Ascend910` and `AscendReal` annotations were
`Ascend910-2,Ascend910-3`, while the approved XR allocation was
`Ascend910-8,Ascend910-9`; `huawei.com/kltDev` was `Ascend910-1,Ascend910-12`.
The node selectors (`module-a3-16`, A3 hostname, ARM64, Ascend910) and the
head's skip-Ascend-plugin annotation were present. This is the previously
documented Volcano/MindX static-device mapping problem, not an omitted
nodeSelector. No real `/v1/models` or chat inference was attempted on the
misallocated worker.

## Small fixes completed during the attempt

- The first Start used saved configuration v2 (one worker, TP=2) and passed the
  capacity gate, but the Direct Operations renderer copied `workerReplicas: 0`
  from the stopped RuntimeProfile baseline. It created only a head, which
  remained Pending in a one-member Volcano PodGroup. The Retain cache PV also
  referenced an old PVC UID; its stale `claimRef` was removed after checking
  the exact PV name, reclaim policy, path, node affinity, and resourceVersion.
  The new PVC bound and the cache Job completed. The first Start was returned
  to Stopped without deleting the cache data.
- Backstage Gitea PR `gitadmin/platform-backstage#17` gained commit
  `6601e6276ac5309990cf0b210e2f69834b3ef8bb`. It sets the Direct Start
  worker count from the saved request, allows Stop to supersede an active
  Start, and makes the recipe UI select the saved TP=2 profile instead of the
  first (8-NPU) catalog variant. Backend tests 5/5, page tests 10/10, app and
  backend builds passed.
- Build-context SHA256:
  `995ca7e785acae4a5e37458c5ea67008a6821ef1b321a006fbaee9d8a89e57af`.
  AMD64 image
  `110.120.0.3:30670/container-images/platform/kcc-backstage:0.6.23-direct-accept-6601e62`
  was published and pulled at digest
  `sha256:dcc269a6156d9b02aa32d8f9a9e0aefa11996abe742038d44335ea401cdeb898`.
  Production Backstage revision 72 is `1/1 Ready`; `/healthcheck`, both model
  pages, `/data-pipeline` and the deployment-status API returned HTTP 200.
  Revision 71, digest
  `sha256:9c6381eb89d2f69e66f4804f6bdeda0a8a07b1c0296ae53793c8a5c1d27e70e3`,
  remains the explicit image rollback target.

## Second Start and safe stop

The second Start request `64f17029-9c39-4fda-a897-84d04fe786ac` reached XR
generation 88, `desiredState=Running`, `workerReplicas=1`, with the approved
static allocation 8/9. Volcano PodGroup was Running with minMember 2; head
and worker bound to A3. The mismatched actual device annotations above were
observed before model load. The Running Window was closed immediately, then
Direct Stop request `9cb8db00-6ec0-4353-9a72-b1cad8d2bc7a` was accepted
while Start was still in ModelLoading.

Final verification: XR generation 91 is `Stopped`, `workerReplicas=0`, using
`modeldeployment-stopped-v2`, with Synced and Ready true. `Responsive=False`
remains as a Crossplane `WatchCircuitOpen` warning for the composed Service
after the rapid Start/Stop event burst; this did not leave a serving resource.
The portal shows
Stopped, zero NPU requested, and no active operation. No RayService,
PodGroup, head, worker, cache Job Pod, or cache PVC remains. The cache PV is
`Released` with `Retain` policy; its data was not deleted. The A3 exporter
showed process count zero for all 16 device IDs after Stop. The Running Window
is `false`.

## Required decision before another NPU Start

Do not reopen the Running Window or retry on the current Volcano/MindX mapping.
The next production step needs a separately reviewed correction of the global
KubeRay/Volcano/device-plugin allocation path (or an approved temporary
operator scheduling override), followed by a fresh capacity check and an
assertion that actual worker devices exactly equal the XR static allocation
before model load. This exceeds the small Direct Operations fixes in this
acceptance run.

## Approved default-scheduler retry

The separately approved retry temporarily removed only
`--batch-scheduler=volcano` from `ray-mangement/kuberay-operator`. The
Deployment returned to `1/1 Ready`; the three pre-existing RayClusters and all
of their Pod UIDs, readiness and restart counts stayed unchanged. The fresh
capacity check selected `Ascend910-8,Ascend910-9` with zero exporter processes.

Direct Start request `3148ef13-b7e6-421b-a717-bf6ebd8488fe` reached one head
and one worker on A3 with `default-scheduler` and no Volcano PodGroup. The
worker's `huawei.com/Ascend910` and `huawei.com/AscendReal` annotations were
both exactly `Ascend910-8,Ascend910-9`; the container exposed only
`/dev/davinci8` and `/dev/davinci9`. This proves the prior mismatch was in the
Volcano/MindX path, while the A3 nodeSelector and static allocation contract
were correct.

The acceptance then stopped before `/v1/models` because the Ray head entered
`CrashLoopBackOff` with `reason=OOMKilled`. Its rendered memory limit was
`8Gi`, while the reviewed production ModelDeployment baseline is `16Gi`.
The TP2 RuntimeProfile omitted explicit head resources, so Direct Operations
used its generic `8Gi` fallback. The worker also restarted once after the head
failure; no inference request was issued.

The Running Window was closed and Direct Stop request
`82d155a2-fd96-4b7a-b5ce-22d771cea855` returned generation 97 to Stopped with
zero workers. The orphaned completed cache Pod was deleted after verifying it
had no owner; the deleting PVC then completed. No Qwen RayService, Pod,
PodGroup or PVC remains. The cache PV data was retained. The exact Volcano
argument and complete Operator Deployment spec were restored, all other
RayClusters remained Ready with their original Pod UIDs/restart counts, and a
final capacity check reported zero processes on all configured devices.

Gitea `gitadmin/model-platform-config#62` fixed the catalog and was merged to
`main`. It explicitly pins head `2 CPU / 16Gi` and worker
`48 CPU / 256Gi`, and extends the RuntimeProfile schema for those resource
fields. Catalog validation passed for two model versions and three profiles,
ModelDeployment validation passed, unit tests passed 6/6, and the merged main
branch was visually verified. A new Start requires the separate approval
record referenced above.

## Final approved retry — passed

The final approved retry completed the Direct Operations production acceptance.
Before Start, `qwen38-27b` was Stopped, the Running Window was false, all Qwen
serving resources were absent, the three other RayClusters were Ready with
their baseline Pod UIDs/restart counts, and the capacity checker reported no
host processes while selecting 8/9. The KubeRay Operator was temporarily moved
from Volcano to the default scheduler with an optimistic-lock patch and
returned to `1/1 Ready`.

Start request `60f35a0c-4b4c-4c36-ba78-5adaab88c55f` rendered generation 100
with head `2 CPU / 16Gi`, one worker at `48 CPU / 256Gi`, and static allocation
`Ascend910-8,Ascend910-9`. Both Pods used `default-scheduler`, had the complete
A3 nodeSelector, and stayed at restart count zero. The worker's
`huawei.com/Ascend910` and `huawei.com/AscendReal` annotations were exactly
8/9, and only `/dev/davinci8` and `/dev/davinci9` were exposed in the worker.
No Volcano PodGroup was created.

The model loaded all ten safetensor shards, completed 33/33 full-decode graph
captures, started the vLLM engine, and reached RayService `Running`. The
EndpointSlice reported `10.42.17.42:8000` ready and serving. The portal showed
Running, one of one worker, Healthy, Service Ready, placement 8/9, and all
pipeline stages through Healthy complete.

The stable service path returned HTTP 200 from `/v1/models` with model ID
`qwen3.8-27b-w8a8`. A real `/v1/chat/completions` request also returned HTTP
200 and usage `57` prompt tokens, `8` completion tokens, `65` total tokens.
The deliberately small `max_tokens=8` stopped the textual response at length;
transport, routing, tokenizer, model execution and token generation all
completed successfully, and no additional inference request was issued.

The Running Window was then closed. Direct Stop request
`352b4cb5-f396-4e0f-987d-d6f442d22541` returned generation 103 to Stopped with
zero workers. The completed cache Pod was verified `Succeeded`, ownerless and
only blocking its deleting PVC before it was removed; the PVC then deleted.
No Qwen RayService, Pod, PodGroup or PVC remains. The Retain cache PV remains
Released at `/home/model-platform/cache/qwen38-27b-w8a8`, preserving data.

The Operator's complete `.spec` compares equal to the pre-test snapshot and
again contains `--batch-scheduler=volcano`. The three unrelated RayClusters
remain Ready with the same Pod UIDs and restart counts. The final capacity
check returned no host processes and again selected 8/9. Crossplane reports
Synced and Ready true; `Responsive=False/WatchCircuitOpen` is the known
post-burst event throttle after rapid Start/Stop and has left no serving
resource. The production acceptance is therefore complete and cleaned.

## 2026-09-20 cache cleanup follow-up

The manual orphan-cache-Pod cleanup observed in this acceptance now has a
production automation fix. Gitea PR `gitadmin/model-platform-config#63`
enabled the bounded cache reaper; its first and subsequent scheduled Jobs
completed successfully without finding a stale object in the stopped
baseline. See
`model-deployment-cache-reaper-production-release-20260920.md` for the exact
selection gates, least-privilege RBAC, rollback and production evidence.
