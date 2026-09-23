# KCC import provenance

This directory was organized from the reviewed Material snapshot
`39515fd2ac4760626baeeec566428cab86ae506a` on 2026-09-23. It intentionally
preserves non-secret platform source, declarative release inputs, architecture,
current-state documents, and acceptance evidence under one KCC module.

## Deliberate exclusions

- Runtime credentials, kubeconfigs, rendered Secrets, Docker auth data,
  local dependency trees, logs, caches, databases, and generated state are
  excluded by design. Every inline credential-like example value in the
  vendored Artifact Keeper values files is blank.
- Generated Artifact Keeper chart documentation is not copied. The vendored
  chart source, explicit values, and the module-level release guidance are the
  maintained source of truth.
- Untracked draft files in the Material working tree are not part of this
  import; only reviewed commits are imported.

## Independent KCC tracks

This module integrates with, but does not duplicate, the independently reviewed
K12 data-pipeline runtime and training-controller deliveries. Until those
modules are present in the selected KCC branch, their platform-side CI and
Backstage integration inputs are declarative dependencies rather than a claim
that the runtime code has been merged.

## Migration rule

Do not treat the import as a production apply. Reconcile a target environment's
namespaces, storage classes, image digests, Secret references, and approved
release gates before any Helm, Kustomize, Argo CD, or Kubernetes operation.
