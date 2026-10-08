"""Idempotent v1 Artifact Gateway publisher."""

from __future__ import annotations

import tempfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from .artifact_publish import MEDIA_TYPE, MAX_RESPONSE_BYTES, PackagedDirectory, _validated_receipt, package_directory
from .artifacts import ArtifactError, ArtifactGateway, ArtifactRef


def upload(gateway: ArtifactGateway, ref: ArtifactRef, package: PackagedDirectory) -> str:
    url = (
        f"{gateway.endpoint}/v1/artifacts/{quote(ref.namespace, safe='')}/"
        f"{quote(ref.name, safe='')}/versions/{quote(ref.version, safe='')}"
    )
    headers = {
        "Accept": "application/json",
        "Content-Type": MEDIA_TYPE,
        "Content-Length": str(package.size),
        "X-Content-SHA256": package.sha256,
        "If-None-Match": "*",
    }
    if gateway.token:
        headers["Authorization"] = f"Bearer {gateway.token}"
    try:
        with package.path.open("rb") as stream:
            request = Request(url, data=stream, method="PUT", headers=headers)
            with urlopen(request, timeout=3600) as response:
                payload = response.read(MAX_RESPONSE_BYTES + 1)
    except HTTPError as error:
        if error.code == 412:
            existing = gateway.resolve(ref)
            if (
                existing.sha256 == package.sha256
                and existing.size == package.size
                and existing.media_type == MEDIA_TYPE
            ):
                return ref.uri
            raise ArtifactError("artifact version already exists with different content") from error
        raise ArtifactError(f"artifact upload returned {error.code}") from error
    except (OSError, URLError) as error:
        raise ArtifactError(f"artifact upload failed: {error}") from error
    return _validated_receipt(payload, ref, package)


def publish_directory(
    gateway: ArtifactGateway,
    namespace: str,
    name: str,
    root: Path,
    attempt: int,
) -> str:
    staging_root = root.parent / ".kcc-publish"
    staging_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f".{root.name}-", dir=staging_root) as temporary:
        packaged = package_directory(root, Path(temporary) / "output.tar.gz")
        version = f"attempt-{attempt:02d}-{packaged.sha256[:16]}"
        return upload(gateway, ArtifactRef(namespace, name, version), packaged)
