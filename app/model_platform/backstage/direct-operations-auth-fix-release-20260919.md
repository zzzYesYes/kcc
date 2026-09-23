# Direct Operations authenticated fetch release — 2026-09-19

## Scope and provenance

The new model recipe and deployment pages called Direct Operations with native
`fetch`, omitting the Backstage identity token and causing HTTP 401 on the
configuration endpoint for a signed-in operator. The release uses Backstage
`fetchApiRef` for configuration GET/PUT and Start/Stop. It changes four frontend
files only; the backend and running-gate policy are unchanged.

- Gitea PR: `gitadmin/platform-backstage#17`, one commit against the already
  deployed `feat/k12-data-pipeline-catalog` branch.
- Source revision: `34820d93c7e1ae70f10ca6e72f10e8281fa6e2b8`.
- `git archive` build-context SHA256:
  `6559445fccee634427595942174793a309d7b479d388f9ce8d01de0c6737f253`.
- Image tag: `110.120.0.3:30670/container-images/platform/kcc-backstage:0.6.22-direct-auth-34820d9`.
- Published and pulled image digest:
  `sha256:9c6381eb89d2f69e66f4804f6bdeda0a8a07b1c0296ae53793c8a5c1d27e70e3`.
- Architecture: `linux/amd64`; OCI revision matches the source revision.
- The ten affected page tests passed and `yarn workspace app build` passed.
  The production Docker build also rebuilt the app/backend overlay and loaded
  the compiled Direct Operations and data-pipeline backend modules.

## Production apply and acceptance

The pre-release Backstage Deployment was revision 70, `1/1 Ready`, using
`sha256:a0855169dad50d132705216039be1c9d0b97f51099118500c7fa96987883d071`.
That exact image and the earlier revision 69 image were present on `server-00`.
The new image was applied only to `backstage/backstage`, with a successful
server-side dry-run of the image change. Revision 71 rolled out successfully,
is `1/1 Ready` with zero Pod restarts, and runs the new immutable digest.

`/healthcheck`, `/kcc-pretraining`, `/model-recipes`, `/model-deployments`,
`/data-pipeline` and `/api/model-platform/deployments` returned HTTP 200
before and after the rollout. In a browser signed in as `gitadmin`, opening
the model recipe page made the production configuration GET return HTTP 200;
the subsequent conditional GET returned 304. The pre-release 401 is gone.
The Start/Stop and configuration PUT paths are covered by the page tests but
were not invoked in this release, so no production configuration version or
model runtime state was changed.

The Running Window remains `false`; no `qwen38-27b` RayService was created.
This is an interface-authentication release, not the complete NPU production
acceptance. That acceptance still requires a fresh NPU availability check and
the separate controlled start window.

## Rollback

Revision 70 is retained by Kubernetes and points to the exact previous image
digest above. If an issue emerges, use
`sudo k3s kubectl -n backstage rollout undo deployment/backstage --to-revision=70`,
then wait for `rollout status`, verify `/healthcheck` and the six routes, and
restore the prior image pin in this repository. Do not open the inference
Running Window as part of this rollback.
