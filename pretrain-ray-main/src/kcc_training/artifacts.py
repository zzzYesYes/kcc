"""Artifact Gateway client and safe, content-addressed materialization.

Material/Artifact Keeper can implement this small HTTP contract without KCC
depending on its internal API.  Runtime code accepts only ``artifact://`` URIs;
the gateway returns a bounded archive URL and immutable SHA256 digest.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import tarfile
import tempfile
from typing import Any, BinaryIO, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen


MAX_MANIFEST_BYTES = 1024 * 1024
MAX_ARCHIVE_BYTES = 1024 * 1024 * 1024 * 1024
MAX_FILES = 2_000_000


class ArtifactError(RuntimeError):
    pass


@dataclass(frozen=True)
class ArtifactRef:
    namespace: str
    name: str
    version: str

    @classmethod
    def parse(cls, uri: str) -> "ArtifactRef":
        parsed = urlparse(uri)
        parts = [part for part in parsed.path.split("/") if part]
        if (
            parsed.scheme != "artifact"
            or not parsed.netloc
            or len(parts) != 2
            or parsed.query
            or parsed.fragment
            or any(part in {".", ".."} for part in parts)
        ):
            raise ArtifactError("artifact URI must be artifact://namespace/name/version")
        fields = (parsed.netloc, parts[0], parts[1])
        if any(not value or len(value) > 255 or any(character.isspace() for character in value) for value in fields):
            raise ArtifactError("artifact URI fields are invalid")
        return cls(namespace=fields[0], name=fields[1], version=fields[2])

    @property
    def uri(self) -> str:
        return f"artifact://{self.namespace}/{self.name}/{self.version}"


@dataclass(frozen=True)
class ResolvedArtifact:
    ref: ArtifactRef
    download_url: str
    sha256: str
    size: int
    media_type: str


class ArtifactGateway:
    def __init__(self, endpoint: str, *, token: str | None = None) -> None:
        parsed = urlparse(endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ArtifactError("artifact gateway endpoint must be HTTP(S)")
        self.endpoint = endpoint.rstrip("/")
        self.token = token

    def resolve(self, ref: ArtifactRef) -> ResolvedArtifact:
        url = (
            f"{self.endpoint}/v1/artifacts/{quote(ref.namespace, safe='')}/"
            f"{quote(ref.name, safe='')}/versions/{quote(ref.version, safe='')}"
        )
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            with urlopen(Request(url, headers=headers), timeout=30) as response:
                payload = response.read(MAX_MANIFEST_BYTES + 1)
        except HTTPError as error:
            raise ArtifactError(f"artifact resolve returned {error.code}") from error
        except (OSError, URLError) as error:
            raise ArtifactError(f"artifact resolve failed: {error}") from error
        if len(payload) > MAX_MANIFEST_BYTES:
            raise ArtifactError("artifact resolve response is too large")
        try:
            value = json.loads(payload)
        except json.JSONDecodeError as error:
            raise ArtifactError("artifact resolve response is invalid JSON") from error
        return resolved_from_mapping(ref, value)


def resolved_from_mapping(ref: ArtifactRef, value: Any) -> ResolvedArtifact:
    if not isinstance(value, Mapping):
        raise ArtifactError("artifact resolve response is not an object")
    download_url = value.get("downloadUrl")
    digest = value.get("sha256")
    size = value.get("size")
    media_type = value.get("mediaType")
    if (
        not isinstance(download_url, str)
        or urlparse(download_url).scheme not in {"http", "https"}
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or isinstance(size, bool)
        or not isinstance(size, int)
        or not 0 < size <= MAX_ARCHIVE_BYTES
        or media_type != "application/vnd.kcc.directory+tar.gz"
    ):
        raise ArtifactError("artifact resolve fields are invalid")
    return ResolvedArtifact(ref, download_url, digest, size, media_type)


def download(resolved: ResolvedArtifact, destination: Path) -> Path:
    digest = hashlib.sha256()
    written = 0
    try:
        with urlopen(Request(resolved.download_url), timeout=300) as response, destination.open("xb") as stream:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                written += len(chunk)
                if written > resolved.size or written > MAX_ARCHIVE_BYTES:
                    raise ArtifactError("artifact download exceeds declared size")
                digest.update(chunk)
                stream.write(chunk)
            stream.flush()
            os.fsync(stream.fileno())
    except ArtifactError:
        raise
    except (OSError, URLError) as error:
        raise ArtifactError(f"artifact download failed: {error}") from error
    if written != resolved.size or digest.hexdigest() != resolved.sha256:
        raise ArtifactError("artifact size or SHA256 differs from gateway manifest")
    return destination


def safe_extract(archive: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=False)
    total = 0
    seen: set[PurePosixPath] = set()
    try:
        with tarfile.open(archive, mode="r:gz") as bundle:
            members = bundle.getmembers()
            if len(members) > MAX_FILES:
                raise ArtifactError("artifact archive contains too many entries")
            for member in members:
                path = PurePosixPath(member.name)
                if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
                    raise ArtifactError("artifact archive contains an unsafe path")
                if path in seen:
                    raise ArtifactError("artifact archive contains a duplicate path")
                seen.add(path)
                if member.issym() or member.islnk() or member.isdev() or member.isfifo():
                    raise ArtifactError("artifact archive contains a link or special file")
                if not (member.isfile() or member.isdir()):
                    raise ArtifactError("artifact archive contains an unsupported entry")
                total += member.size
                if total > MAX_ARCHIVE_BYTES:
                    raise ArtifactError("artifact uncompressed size exceeds the limit")
            for member in members:
                target = destination.joinpath(*PurePosixPath(member.name).parts)
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                    target.chmod(0o755)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                source = bundle.extractfile(member)
                if source is None:
                    raise ArtifactError("artifact regular file has no payload")
                with source, target.open("xb") as output:
                    shutil.copyfileobj(source, output, length=1024 * 1024)
                    output.flush()
                    os.fsync(output.fileno())
                target.chmod(0o644 | (0o111 if member.mode & 0o111 else 0))
    except ArtifactError:
        raise
    except (OSError, tarfile.TarError) as error:
        raise ArtifactError(f"cannot extract artifact: {error}") from error


def _marker_matches(destination: Path, resolved: ResolvedArtifact) -> bool:
    marker = destination / ".kcc-artifact.json"
    if not marker.is_file():
        return False
    try:
        existing = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ArtifactError(f"existing artifact marker is invalid: {error}") from error
    return existing.get("uri") == resolved.ref.uri and existing.get("sha256") == resolved.sha256


def materialize(
    resolved: ResolvedArtifact,
    destination: Path,
    *,
    downloader: Callable[[ResolvedArtifact, Path], Path] = download,
) -> Path:
    if _marker_matches(destination, resolved):
        return destination
    if destination.exists() or destination.is_symlink():
        raise ArtifactError("artifact destination is already owned by another digest")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_root = Path(tempfile.mkdtemp(prefix=f".{destination.name}.kcc-", dir=destination.parent))
    archive = temporary_root / "artifact.tar.gz"
    extracted = temporary_root / "content"
    try:
        downloader(resolved, archive)
        safe_extract(archive, extracted)
        marker_payload = {"uri": resolved.ref.uri, "sha256": resolved.sha256, "size": resolved.size}
        (extracted / ".kcc-artifact.json").write_text(
            json.dumps(marker_payload, sort_keys=True) + "\n", encoding="utf-8"
        )
        try:
            extracted.rename(destination)
        except OSError as error:
            # Another init container may have completed the same immutable URI.
            if _marker_matches(destination, resolved):
                return destination
            raise ArtifactError(f"cannot commit materialized artifact: {error}") from error
        return destination
    finally:
        shutil.rmtree(temporary_root, ignore_errors=True)

