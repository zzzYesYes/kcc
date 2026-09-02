# KCC architecture

KCC is organized as independently reviewable platform modules. Monitoring,
data-pipeline runtime, developer portal, and resource-management integration
have separate ownership boundaries.

The full model-platform integration architecture, including Artifact Keeper,
Gitea, Tekton, Argo CD, Crossplane, Backstage, and KubeRay responsibilities,
is maintained in
[`app/model_platform/TARGET-ARCHITECTURE.md`](../app/model_platform/TARGET-ARCHITECTURE.md).

Current production facts and release gates are intentionally separated from
this architecture and live in the same module.
