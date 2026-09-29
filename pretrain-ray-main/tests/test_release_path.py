import io
from dataclasses import replace
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from kcc_training.api_v1beta1 import Run
from kcc_training.artifact_publish import PackagedDirectory, _validated_receipt, package_directory
from kcc_training.artifacts import ArtifactError, ArtifactRef
from kcc_training.controller import ControllerError
from kcc_training.controller_release import trusted_result
from kcc_training.manifests_release import render_attempt
from kcc_training.ray_jobs_release import ReleaseRayJobsRest
from kcc_training.ray_jobs_rest import RestResponse
from tests.test_api_v1beta1 import resource
from tests.test_raycluster import objects


class Transport:
    def __init__(self):
        self.calls = []
        self.responses = [RestResponse(404, b"{}"), RestResponse(200, b'{"submission_id":"run-a00"}')]

    def __call__(self, method, url, body):
        self.calls.append((method, url, body))
        return self.responses.pop(0)


def run_model():
    return Run.from_resource(resource("TrainingRun", "run-1", {
        "runtimeProfile": "profile", "recipe": "recipe", "workers": 1,
        "recovery": {"sameTopologyRetries": 0, "maxReplacements": 0, "noProgressSeconds": 0},
    }))


def result_document(result):
    return {
        "metadata": {"annotations": {
            "training.kcc.io/run-uid": "uid-1", "training.kcc.io/attempt": "0",
        }},
        "data": {"result.json": json.dumps(result)},
    }


class ReleasePathTests(unittest.TestCase):
    def test_output_archive_is_reproducible_and_has_canonical_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "output"
            root.mkdir()
            (root / "train.log").write_text("done\n", encoding="utf-8")
            first = package_directory(root, Path(temporary) / "first.tar.gz")
            second = package_directory(root, Path(temporary) / "second.tar.gz")
            self.assertEqual((first.sha256, first.size), (second.sha256, second.size))
            with tarfile.open(first.path, "r:gz") as archive:
                item = archive.getmember("train.log")
                self.assertEqual((item.uid, item.gid, item.mtime, item.mode), (0, 0, 0, 0o644))

    def test_output_archive_rejects_symlinks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "output"
            root.mkdir()
            (root / "outside").symlink_to("/etc/passwd")
            with self.assertRaisesRegex(ArtifactError, "link"):
                package_directory(root, Path(temporary) / "output.tar.gz")

    def test_gateway_receipt_must_match_uploaded_bytes(self):
        package = PackagedDirectory(Path("x"), "a" * 64, 42)
        ref = ArtifactRef("training", "run-1-output", "attempt-00-deadbeef")
        good = json.dumps({"uri": ref.uri, "sha256": package.sha256, "size": 42}).encode()
        self.assertEqual(_validated_receipt(good, ref, package), ref.uri)
        with self.assertRaisesRegex(ArtifactError, "differs"):
            _validated_receipt(b'{"uri":"artifact://wrong/x/y","sha256":"x","size":1}', ref, package)

    def test_pass_attestation_requires_checkpoint_and_owned_output(self):
        base = {
            "schemaVersion": "kcc-runtime-result/v1", "runUid": "uid-1", "attempt": 0,
            "status": "PASS", "checkpointConsistent": True,
            "outputArtifact": "artifact://training/run-1-output/attempt-00-0123456789abcdef",
        }
        accepted = trusted_result(result_document(base), run_model(), 0)
        self.assertEqual(accepted["outputArtifact"], base["outputArtifact"])
        invalid = dict(base, checkpointConsistent=False)
        with self.assertRaisesRegex(ControllerError, "checkpoint"):
            trusted_result(result_document(invalid), run_model(), 0)
        invalid = dict(base, outputArtifact="artifact://training/other/attempt-00-0123456789abcdef")
        with self.assertRaisesRegex(ControllerError, "ownership"):
            trusted_result(result_document(invalid), run_model(), 0)

    def test_release_ray_job_uses_publishing_coordinator(self):
        transport = Transport()
        ReleaseRayJobsRest(transport).submit_once(
            "http://ray", "run-a00", ("python", "train.py"), metadata={"runUid": "uid-1"}
        )
        payload = json.loads(transport.calls[1][2])
        self.assertIn("coordinator_release", payload["entrypoint"])

    def test_head_receives_gateway_and_read_only_token(self):
        environment = {
            "KCC_ARTIFACT_GATEWAY": "https://artifacts.internal",
            "KCC_ARTIFACT_TOKEN_SECRET": "artifact-token",
        }
        with patch.dict(os.environ, environment, clear=True):
            _configmap, cluster = render_attempt(
                *objects(), attempt=0, active_nodes=("node-a", "node-b"), runtime_service_account="runtime"
            )
        head = cluster["spec"]["headGroupSpec"]["template"]["spec"]["containers"][0]
        self.assertIn({"name": "KCC_ARTIFACT_GATEWAY", "value": environment["KCC_ARTIFACT_GATEWAY"]}, head["env"])
        token = next(item for item in head["volumeMounts"] if item["name"] == "artifact-token")
        self.assertTrue(token["readOnly"])

    def test_workspace_head_has_no_gateway_dependency(self):
        run, profile, recipe = objects()
        profile = replace(profile, artifact_provider="workspace")
        with patch.dict(os.environ, {}, clear=True):
            _configmap, cluster = render_attempt(
                run,
                profile,
                recipe,
                attempt=0,
                active_nodes=("node-a", "node-b"),
                runtime_service_account="runtime",
            )
        head_spec = cluster["spec"]["headGroupSpec"]["template"]["spec"]
        head = head_spec["containers"][0]
        self.assertNotIn("initContainers", head_spec)
        self.assertNotIn("KCC_ARTIFACT_GATEWAY", str(head.get("env", [])))


if __name__ == "__main__":
    unittest.main()
