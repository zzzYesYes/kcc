# Resource management integration

The KCC model-deployment API and its constrained Crossplane composition are
implemented in [`../model_platform/crossplane`](../model_platform/crossplane).
They turn reviewed GitOps `ModelDeployment` requests into bounded cache and
KubeRay resources.

Stopped requests are the safe baseline. Running/NPU activation remains gated
by approval, immutable artifact checks, capacity, and the release policy
documented in the model-platform module.
