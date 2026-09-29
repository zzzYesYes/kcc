from pathlib import Path
import hashlib
import io
import json
import tarfile
import tempfile
import unittest

from kcc_training.artifacts import ArtifactError, ArtifactRef, ResolvedArtifact, materialize, safe_extract


def archive(path: Path, name: str = "file.txt", payload: bytes = b"hello") -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as bundle:
        info = tarfile.TarInfo(name)
        info.size = len(payload)
        bundle.addfile(info, io.BytesIO(payload))
    value = buffer.getvalue()
    path.write_bytes(value)
    return value


class ArtifactTests(unittest.TestCase):
    def test_uri_is_versioned(self):
        ref = ArtifactRef.parse("artifact://models/qwen3/v1")
        self.assertEqual(ref.namespace, "models")
        with self.assertRaises(ArtifactError):
            ArtifactRef.parse("artifact://models/qwen3")

    def test_safe_extract_rejects_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bad = root / "bad.tar.gz"
            archive(bad, "../outside")
            with self.assertRaisesRegex(ArtifactError, "unsafe path"):
                safe_extract(bad, root / "output")

    def test_materialization_is_digest_owned_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.tar.gz"
            payload = archive(source)
            ref = ArtifactRef.parse("artifact://source/code/v1")
            resolved = ResolvedArtifact(ref, "https://invalid", hashlib.sha256(payload).hexdigest(), len(payload), "application/vnd.kcc.directory+tar.gz")
            def copy(_resolved, destination):
                destination.write_bytes(source.read_bytes())
                return destination
            destination = root / "materialized"
            materialize(resolved, destination, downloader=copy)
            self.assertEqual((destination / "file.txt").read_bytes(), b"hello")
            self.assertEqual(json.loads((destination / ".kcc-artifact.json").read_text())["sha256"], resolved.sha256)
            self.assertEqual(materialize(resolved, destination, downloader=copy), destination)


if __name__ == "__main__":
    unittest.main()
