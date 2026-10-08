"""Deterministic output packaging and the Artifact Gateway upload contract."""

from __future__ import annotations

from dataclasses import dataclass
import gzip
import hashlib
import json
import os
from pathlib import Path
import stat
import tarfile
import tempfile
from typing import Any, BinaryIO, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from .artifacts import ArtifactError, ArtifactGateway, ArtifactRef, MAX_ARCHIVE_BYTES, MAX_FILES


MEDIA_TYPE = "application/vnd.kcc.directory+tar.gz"
MAX_RESPONSE_BYTES = 1024 * 1024


@dataclass(frozen=True)
class PackagedDirectory:
    path: Path
    sha256: str
    size: int


def _entries(root: Path) -> list[Path]:
    if not root.is_dir() or root.is_symlink():
        raise ArtifactError("output root must be a real directory")
    values: list[Path] = []
    for directory, directory_names, file_names in os.walk(root, topdown=True, followlinks=False):
        current = Path(directory)
        for name in sorted(directory_names):
            values.append(current / name)
        for name in sorted(file_names):
            values.append(current / name)
        directory_names.sort()
    if len(values) > MAX_FILES:
        raise ArtifactError("output contains too many entries")
    return sorted(values, key=lambda item: item.relative_to(root).as_posix())


def package_directory(root: Path, destination: Path) -> PackagedDirectory:
    """Create a reproducible archive while refusing links and special files."""
    entries = _entries(root)
    total = 0
    with destination.open("xb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(mode="w", fileobj=compressed, format=tarfile.PAX_FORMAT) as bundle:
                for path in entries:
                    info_stat = path.lstat()
                    if stat.S_ISLNK(info_stat.st_mode) or not (
                        stat.S_ISREG(info_stat.st_mode) or stat.S_ISDIR(info_stat.st_mode)
                    ):
                        raise ArtifactError("output contains a link or special file")
                    relative = path.relative_to(root).as_posix()
                    info = tarfile.TarInfo(relative + ("/" if path.is_dir() else ""))
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    info.mtime = 0
                    info.mode = 0o755 if path.is_dir() or info_stat.st_mode & 0o111 else 0o644
                    if path.is_dir():
                        info.type = tarfile.DIRTYPE
                        bundle.addfile(info)
                    else:
                        total += info_stat.st_size
                        if total > MAX_ARCHIVE_BYTES:
                            raise ArtifactError("output exceeds the artifact size limit")
                        info.size = info_stat.st_size
                        with path.open("rb") as stream:
                            bundle.addfile(info, stream)
        raw.flush()
        os.fsync(raw.fileno())
    size = destination.stat().st_size
    if size <= 0 or size > MAX_ARCHIVE_BYTES:
        raise ArtifactError("packaged output size is invalid")
    digest = hashlib.sha256()
    with destination.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return PackagedDirectory(destination, digest.hexdigest(), size)


def _validated_receipt(payload: bytes, ref: ArtifactRef, package: PackagedDirectory) -> str:
    if len(payload) > MAX_RESPONSE_BYTES:
        raise ArtifactError("artifact upload response is too large")
    try:
        value: Any = json.loads(payload)
    except json.JSONDecodeError as error:
        raise ArtifactError("artifact upload response is invalid JSON") from error
    if not isinstance(value, Mapping) or (
        value.get("uri") != ref.uri
        or value.get("sha256") != package.sha256
        or value.get("size") != package.size
    ):
        raise ArtifactError("artifact upload receipt differs from the uploaded content")
    return ref.uri


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
        raise ArtifactError(f"artifact upload returned {error.code}") from error
    except (OSError, URLError) as error:
        raise ArtifactError(f"artifact upload failed: {error}") from error
    return _validated_receipt(payload, ref, package)


def publish_directory(gateway: ArtifactGateway, namespace: str, name: str, root: Path, attempt: int) -> str:
    staging_root = root.parent / ".kcc-publish"
    staging_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f".{root.name}-", dir=staging_root) as temporary:
        packaged = package_directory(root, Path(temporary) / "output.tar.gz")
        version = f"attempt-{attempt:02d}-{packaged.sha256[:16]}"
        return upload(gateway, ArtifactRef(namespace, name, version), packaged)
