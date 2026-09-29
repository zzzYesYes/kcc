# Kubernetes Convenient Cluster

KCC is an opinionated collection of cloud-native components for an on-premise
Kubernetes cluster that serves, pre-trains, post-trains, and operates LLM
workloads.

## Implemented delivery modules

- `app/monitoring/`: monitoring and NPU observability.
- `app/data_pipeline/`: independently delivered K12 data-pipeline runtime.
- `app/model_platform/`: non-secret production integration for artifact
  storage, GitOps, CI policy, model deployment control plane, and the
  constrained developer portal.

The model-platform module is deliberately declarative and fail-closed. It
documents current production facts, target architecture, release gates, and
rollback boundaries; it does not apply a cluster merely by being cloned.

## Quick start

Read the module README and its current-state document before rendering any
chart or manifest. Provision secrets out of band, validate locally, and use
the component-specific release procedure for any approved environment.
