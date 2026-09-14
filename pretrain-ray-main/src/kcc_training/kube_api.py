"""Minimal in-cluster Kubernetes HTTP client with optimistic concurrency.

The controller uses this module instead of mounting kubectl or kubeconfig.  All
updates carry resourceVersion and all owned deletes carry a UID precondition.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from threading import Event, Lock, Thread
import ssl
import time
from typing import Any, Callable, Iterator, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


SERVICE_ACCOUNT_ROOT = Path("/var/run/secrets/kubernetes.io/serviceaccount")


class KubernetesApiError(RuntimeError):
    def __init__(self, status: int | None, message: str) -> None:
        super().__init__(message)
        self.status = status


class KubernetesOwnershipError(KubernetesApiError):
    """An existing object is owned by a different TrainingRun."""


@dataclass(frozen=True)
class ApiResponse:
    status: int
    body: bytes


Transport = Callable[[str, str, bytes | None, Mapping[str, str]], ApiResponse]


def _controller_owner_uid(metadata: Mapping[str, Any]) -> str | None:
    references = metadata.get("ownerReferences")
    if not isinstance(references, list):
        return None

    for reference in references:
        if isinstance(reference, Mapping) and reference.get("controller") is True:
            uid = reference.get("uid")
            return uid if isinstance(uid, str) and uid else None
    return None


class KubernetesApi:
    def __init__(
        self,
        *,
        server: str = "https://kubernetes.default.svc",
        token_path: Path = SERVICE_ACCOUNT_ROOT / "token",
        ca_path: Path = SERVICE_ACCOUNT_ROOT / "ca.crt",
        transport: Transport | None = None,
    ) -> None:
        self._server = server.rstrip("/")
        self._transport_override = transport
        if transport is None:
            try:
                self._token = token_path.read_text(encoding="utf-8").strip()
            except (OSError, UnicodeError) as error:
                raise KubernetesApiError(None, f"cannot read service account token: {error}") from error
            if not self._token:
                raise KubernetesApiError(None, "service account token is empty")
            try:
                self._ssl_context = ssl.create_default_context(cafile=str(ca_path))
            except OSError as error:
                raise KubernetesApiError(None, f"cannot load Kubernetes CA: {error}") from error
        else:
            self._token = ""
            self._ssl_context = None

    def _transport(
        self, method: str, path: str, body: bytes | None, headers: Mapping[str, str]
    ) -> ApiResponse:
        if self._transport_override is not None:
            return self._transport_override(method, path, body, headers)
        request = Request(
            f"{self._server}{path}",
            data=body,
            method=method,
            headers={"Authorization": f"Bearer {self._token}", **headers},
        )
        try:
            with urlopen(request, timeout=30, context=self._ssl_context) as response:
                return ApiResponse(response.status, response.read())
        except HTTPError as error:
            return ApiResponse(error.code, error.read())
        except (OSError, URLError) as error:
            raise KubernetesApiError(None, f"Kubernetes API request failed: {error}") from error

    def request(
        self,
        method: str,
        path: str,
        document: Mapping[str, Any] | None = None,
        *,
        expected: tuple[int, ...] = (200,),
    ) -> Mapping[str, Any] | None:
        body = (
            json.dumps(dict(document), ensure_ascii=False, separators=(",", ":")).encode()
            if document is not None
            else None
        )
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        response = self._transport(method, path, body, headers)
        if response.status not in expected:
            detail = response.body.decode("utf-8", errors="replace")[:2000]
            raise KubernetesApiError(
                response.status,
                f"Kubernetes API {method} {path} returned {response.status}: {detail}",
            )
        if not response.body:
            return None
        try:
            value = json.loads(response.body)
        except json.JSONDecodeError as error:
            raise KubernetesApiError(response.status, f"Kubernetes API returned invalid JSON: {error}") from error
        if not isinstance(value, Mapping):
            raise KubernetesApiError(response.status, "Kubernetes API response is not an object")
        return value

    def get(self, path: str) -> Mapping[str, Any] | None:
        try:
            return self.request("GET", path)
        except KubernetesApiError as error:
            if error.status == 404:
                return None
            raise

    def list(self, path: str, *, labels: str | None = None) -> Mapping[str, Any]:
        query = f"?{urlencode({'labelSelector': labels})}" if labels else ""
        value = self.request("GET", f"{path}{query}")
        if value is None or not isinstance(value.get("items"), list):
            raise KubernetesApiError(200, "Kubernetes list response has invalid items")
        return value

    def create(self, collection_path: str, document: Mapping[str, Any]) -> Mapping[str, Any]:
        value = self.request("POST", collection_path, document, expected=(200, 201))
        if value is None:
            raise KubernetesApiError(201, "Kubernetes create returned no object")
        return value

    def replace(self, item_path: str, document: Mapping[str, Any]) -> Mapping[str, Any]:
        metadata = document.get("metadata")
        if not isinstance(metadata, Mapping) or not metadata.get("resourceVersion"):
            raise KubernetesApiError(None, "replace requires metadata.resourceVersion")
        value = self.request("PUT", item_path, document)
        if value is None:
            raise KubernetesApiError(200, "Kubernetes replace returned no object")
        return value

    def upsert(
        self,
        collection_path: str,
        item_path: str,
        desired: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        current = self.get(item_path)
        if current is None:
            try:
                return self.create(collection_path, desired)
            except KubernetesApiError as error:
                if error.status != 409:
                    raise
                current = self.get(item_path)
                if current is None:
                    raise
        current_metadata = current.get("metadata")
        desired_metadata = desired.get("metadata")
        if not isinstance(current_metadata, Mapping) or not isinstance(desired_metadata, Mapping):
            raise KubernetesApiError(None, "upsert object metadata is invalid")
        desired_annotations = desired_metadata.get("annotations")
        current_annotations = current_metadata.get("annotations")
        desired_owner = (
            desired_annotations.get("training.kcc.io/run-uid")
            if isinstance(desired_annotations, Mapping)
            else None
        )
        current_owner = (
            current_annotations.get("training.kcc.io/run-uid")
            if isinstance(current_annotations, Mapping)
            else None
        )
        if desired_owner is not None and current_owner != desired_owner:
            raise KubernetesOwnershipError(
                409,
                "refusing to replace an object owned by another TrainingRun",
            )
        desired_controller = _controller_owner_uid(desired_metadata)
        current_controller = _controller_owner_uid(current_metadata)
        if desired_controller is not None and current_controller != desired_controller:
            raise KubernetesOwnershipError(
                409,
                "refusing to replace an object with a different controller owner UID",
            )
        replacement = dict(desired)
        replacement["metadata"] = {
            **dict(desired_metadata),
            "resourceVersion": current_metadata.get("resourceVersion"),
            **(
                {"finalizers": list(current_metadata["finalizers"])}
                if "finalizers" in current_metadata and "finalizers" not in desired_metadata
                else {}
            ),
        }
        return self.replace(item_path, replacement)

    def update_status(self, item_path: str, current: Mapping[str, Any], status: Mapping[str, Any]) -> Mapping[str, Any]:
        metadata = current.get("metadata")
        if not isinstance(metadata, Mapping) or not metadata.get("resourceVersion"):
            raise KubernetesApiError(None, "status update requires resourceVersion")
        body = {
            "apiVersion": current.get("apiVersion"),
            "kind": current.get("kind"),
            "metadata": {
                "name": metadata.get("name"),
                "namespace": metadata.get("namespace"),
                "resourceVersion": metadata.get("resourceVersion"),
            },
            "status": dict(status),
        }
        value = self.request("PUT", f"{item_path}/status", body)
        if value is None:
            raise KubernetesApiError(200, "status update returned no object")
        return value

    def delete_owned(self, item_path: str, uid: str) -> None:
        if not uid:
            raise KubernetesApiError(None, "owned delete requires a UID")
        self.request(
            "DELETE",
            item_path,
            {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "propagationPolicy": "Foreground",
                "preconditions": {"uid": uid},
            },
            expected=(200, 202, 404),
        )


def namespaced_path(group: str, version: str, namespace: str, plural: str, name: str | None = None) -> str:
    base = (
        f"/apis/{quote(group, safe='')}/{quote(version, safe='')}/namespaces/"
        f"{quote(namespace, safe='')}/{quote(plural, safe='')}"
    )
    return f"{base}/{quote(name, safe='')}" if name else base


def core_namespaced_path(namespace: str, plural: str, name: str | None = None) -> str:
    base = f"/api/v1/namespaces/{quote(namespace, safe='')}/{quote(plural, safe='')}"
    return f"{base}/{quote(name, safe='')}" if name else base


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class LeaseLock:
    def __init__(
        self,
        api: KubernetesApi,
        *,
        namespace: str,
        name: str,
        identity: str,
        duration_seconds: int = 30,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._api = api
        self._collection = namespaced_path("coordination.k8s.io", "v1", namespace, "leases")
        self._item = namespaced_path("coordination.k8s.io", "v1", namespace, "leases", name)
        self._namespace = namespace
        self._name = name
        self._identity = identity
        self._duration = max(2, duration_seconds)
        self._clock = clock
        self._state_lock = Lock()
        self._held = False
        self._held_until = 0.0

    @property
    def held(self) -> bool:
        with self._state_lock:
            if self._held and self._clock() >= self._held_until:
                self._held = False
            return self._held

    def _mark_held(self, now: float) -> None:
        with self._state_lock:
            self._held = True
            self._held_until = now + self._duration

    def _mark_lost(self) -> None:
        with self._state_lock:
            self._held = False

    def _expired_locally(self) -> bool:
        with self._state_lock:
            return self._clock() >= self._held_until

    def acquire_or_renew(self) -> bool:
        now = self._clock()
        now_text = datetime.fromtimestamp(now, timezone.utc).isoformat().replace("+00:00", "Z")
        current = self._api.get(self._item)
        if current is None:
            desired = {
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": self._name, "namespace": self._namespace},
                "spec": {
                    "holderIdentity": self._identity,
                    "leaseDurationSeconds": self._duration,
                    "acquireTime": now_text,
                    "renewTime": now_text,
                    "leaseTransitions": 0,
                },
            }
            try:
                self._api.create(self._collection, desired)
            except KubernetesApiError as error:
                if error.status == 409:
                    self._mark_lost()
                    return False
                raise
            self._mark_held(now)
            return True
        metadata = current.get("metadata")
        spec = current.get("spec")
        if not isinstance(metadata, Mapping) or not isinstance(spec, Mapping):
            raise KubernetesApiError(None, "Lease response is invalid")
        holder = spec.get("holderIdentity")
        renew_text = spec.get("renewTime")
        lease_duration = spec.get("leaseDurationSeconds", self._duration)
        try:
            renew = datetime.fromisoformat(str(renew_text).replace("Z", "+00:00")).timestamp()
            expired = now >= renew + int(lease_duration)
        except (TypeError, ValueError):
            expired = True
        if holder != self._identity and not expired:
            self._mark_lost()
            return False
        replacement = dict(current)
        replacement["spec"] = {
            **dict(spec),
            "holderIdentity": self._identity,
            "leaseDurationSeconds": self._duration,
            "renewTime": now_text,
            "leaseTransitions": int(spec.get("leaseTransitions", 0))
            + (1 if holder != self._identity else 0),
        }
        try:
            self._api.replace(self._item, replacement)
        except KubernetesApiError as error:
            if error.status == 409:
                self._mark_lost()
            raise
        self._mark_held(now)
        return True

    @contextmanager
    def maintain(self) -> Iterator[None]:
        """Renew leadership while a potentially long reconciliation batch runs."""
        if not self.held:
            raise KubernetesApiError(None, "cannot maintain a Lease that is not held")
        stop = Event()
        interval = max(1.0, self._duration / 3)

        def renew_loop() -> None:
            while not stop.wait(interval):
                try:
                    if not self.acquire_or_renew():
                        return
                except KubernetesApiError as error:
                    if error.status == 409 or self._expired_locally():
                        self._mark_lost()
                        return

        thread = Thread(target=renew_loop, name="kcc-lease-renewer", daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join(timeout=35)
            self._mark_lost()
