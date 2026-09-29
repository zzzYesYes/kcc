import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from kcc_training.artifact_publish import MEDIA_TYPE, PackagedDirectory
from kcc_training.artifact_publish_v1 import upload
from kcc_training.artifacts import ArtifactError, ArtifactRef, ResolvedArtifact
from kcc_training.ray_jobs_rest import RestResponse
from kcc_training.ray_jobs_v1 import V1RayJobsRest


class ExistingGateway:
    endpoint = "https://gateway.internal"
    token = None

    def __init__(self, resolved):
        self.resolved = resolved

    def resolve(self, ref):
        self.ref = ref
        return self.resolved


class Transport:
    def __init__(self):
        self.calls = []
        self.responses = [RestResponse(404, b"{}"), RestResponse(200, b'{"submission_id":"run-a00"}')]

    def __call__(self, method, url, body):
        self.calls.append((method, url, body))
        return self.responses.pop(0)


class V1ReleaseTests(unittest.TestCase):
    def test_upload_replay_accepts_only_same_persisted_content(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive = Path(temporary) / "output.tar.gz"
            archive.write_bytes(b"content")
            package = PackagedDirectory(archive, "a" * 64, 7)
            ref = ArtifactRef("training", "run-output", "attempt-00-aaaaaaaaaaaaaaaa")
            resolved = ResolvedArtifact(ref, "https://objects/x", package.sha256, 7, MEDIA_TYPE)
            error = HTTPError("https://gateway", 412, "exists", {}, io.BytesIO())
            with patch("kcc_training.artifact_publish_v1.urlopen", side_effect=error):
                self.assertEqual(upload(ExistingGateway(resolved), ref, package), ref.uri)
            different = ResolvedArtifact(ref, "https://objects/x", "b" * 64, 7, MEDIA_TYPE)
            with patch("kcc_training.artifact_publish_v1.urlopen", side_effect=error):
                with self.assertRaisesRegex(ArtifactError, "different content"):
                    upload(ExistingGateway(different), ref, package)

    def test_v1_ray_job_uses_v1_runtime(self):
        transport = Transport()
        V1RayJobsRest(transport).submit_once(
            "http://ray", "run-a00", ("python", "train.py"), metadata={"runUid": "uid-1"}
        )
        self.assertIn(b"coordinator_v1", transport.calls[1][2])


if __name__ == "__main__":
    unittest.main()
