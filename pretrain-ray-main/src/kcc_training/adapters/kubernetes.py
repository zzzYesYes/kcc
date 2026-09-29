"""Kubernetes adapter using argument arrays and JSON documents only."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .command import CommandError, SubprocessRunner


class KubernetesError(RuntimeError):
    pass


class KubernetesCli:
    def __init__(
        self,
        command: Sequence[str] = ("kubectl",),
        *,
        kubeconfig: Path | None = None,
        runner: SubprocessRunner | None = None,
    ) -> None:
        if not command:
            raise ValueError("kubectl command must not be empty")
        self._prefix = tuple(command) + (
            (("--kubeconfig", str(kubeconfig))) if kubeconfig is not None else ()
        )
        self._runner = runner or SubprocessRunner()

    @staticmethod
    def _decode_json(text: str, label: str) -> Mapping[str, Any]:
        try:
            value = json.loads(text)
        except json.JSONDecodeError as error:
            raise KubernetesError(f"{label} returned invalid JSON: {error}") from error
        if not isinstance(value, Mapping):
            raise KubernetesError(f"{label} did not return a JSON object")
        return value

    def get(self, kind: str, name: str, namespace: str) -> Mapping[str, Any] | None:
        argv = [
            *self._prefix,
            "get",
            kind,
            name,
            "--namespace",
            namespace,
            "--ignore-not-found",
            "--output",
            "json",
        ]
        try:
            result = self._runner.run(argv)
        except CommandError as error:
            raise KubernetesError(str(error)) from error
        if not result.stdout.strip():
            return None
        return self._decode_json(result.stdout, f"{kind}/{name}")

    def list_json(
        self,
        kind: str,
        *,
        namespace: str | None = None,
        all_namespaces: bool = False,
        labels: str | None = None,
    ) -> Mapping[str, Any]:
        if namespace is not None and all_namespaces:
            raise ValueError("namespace and all_namespaces are mutually exclusive")
        argv = [*self._prefix, "get", kind]
        if namespace is not None:
            argv.extend(("--namespace", namespace))
        if all_namespaces:
            argv.append("--all-namespaces")
        if labels is not None:
            argv.extend(("--selector", labels))
        argv.extend(("--output", "json"))
        try:
            result = self._runner.run(argv)
        except CommandError as error:
            raise KubernetesError(str(error)) from error
        return self._decode_json(result.stdout, f"list {kind}")

    def apply(self, manifest: Mapping[str, Any]) -> Mapping[str, Any]:
        payload = json.dumps(dict(manifest), ensure_ascii=False)
        try:
            result = self._runner.run(
                [*self._prefix, "apply", "--filename", "-", "--output", "json"],
                input_text=payload,
            )
        except CommandError as error:
            raise KubernetesError(str(error)) from error
        return self._decode_json(result.stdout, "kubectl apply")

    def delete_owned(
        self, kind: str, name: str, namespace: str, owner_uid: str
    ) -> None:
        current = self.get(kind, name, namespace)
        if current is None:
            return
        metadata = current.get("metadata")
        uid = metadata.get("uid") if isinstance(metadata, Mapping) else None
        if uid != owner_uid:
            raise KubernetesError(
                f"refusing to delete {kind}/{name}: resource UID changed"
            )
        try:
            self._runner.run(
                [
                    *self._prefix,
                    "delete",
                    kind,
                    name,
                    "--namespace",
                    namespace,
                    "--wait=false",
                ]
            )
        except CommandError as error:
            raise KubernetesError(str(error)) from error

