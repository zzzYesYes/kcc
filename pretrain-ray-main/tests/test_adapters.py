from pathlib import Path
import json
import tempfile
import unittest

from kcc_training.adapters.checkpoint import CheckpointEvidenceAdapter
from kcc_training.adapters.command import CommandResult
from kcc_training.adapters.kubernetes import KubernetesCli, KubernetesError
from kcc_training.adapters.ranktable import ConfigMapRankTableAdapter, RankTableError
from kcc_training.adapters.ray_jobs import RayJobsAdapter


class FakeRunner:
    def __init__(self, results: list[CommandResult]) -> None:
        self.results = results
        self.calls: list[tuple[tuple[str, ...], str | None]] = []

    def run(self, argv, *, timeout_seconds=30, input_text=None, check=True):
        del timeout_seconds, check
        self.calls.append((tuple(argv), input_text))
        return self.results.pop(0)


class FakeKubernetes:
    def __init__(self, document):
        self.document = document

    def get(self, kind, name, namespace):
        self.request = (kind, name, namespace)
        return self.document


class FakeRayClient:
    def __init__(self, address: str) -> None:
        self.address = address
        self.status_value = "RUNNING"
        self.submit_error = None
        self.submissions = []

    def submit_job(self, **kwargs):
        self.submissions.append(kwargs)
        if self.submit_error:
            raise self.submit_error
        return kwargs["submission_id"]

    def get_job_status(self, submission_id):
        del submission_id
        return self.status_value

    def stop_job(self, submission_id):
        del submission_id
        return True


class AdapterTests(unittest.TestCase):
    def test_kubernetes_apply_uses_stdin_and_argument_array(self) -> None:
        result = CommandResult(("kubectl",), 0, '{"kind":"ConfigMap"}', "")
        runner = FakeRunner([result])
        client = KubernetesCli(("kubectl",), runner=runner)
        response = client.apply({"kind": "ConfigMap"})
        self.assertEqual(response["kind"], "ConfigMap")
        argv, stdin = runner.calls[0]
        self.assertEqual(argv, ("kubectl", "apply", "--filename", "-", "--output", "json"))
        self.assertEqual(json.loads(stdin), {"kind": "ConfigMap"})

    def test_kubernetes_delete_rejects_changed_uid(self) -> None:
        current = CommandResult(
            ("kubectl",), 0, '{"metadata":{"uid":"new-uid"}}', ""
        )
        client = KubernetesCli(("kubectl",), runner=FakeRunner([current]))
        with self.assertRaisesRegex(KubernetesError, "UID changed"):
            client.delete_owned("raycluster", "train", "training", "old-uid")

    def test_ranktable_validates_owner_and_digest(self) -> None:
        payload = json.dumps(
            {"server_list": [{"server_id": "10.0.0.1"}, {"server_id": "10.0.0.2"}]},
            separators=(",", ":"),
        )
        document = {
            "metadata": {
                "labels": {"app.kubernetes.io/managed-by": "hccl-ranktable-sanitizer"},
                "annotations": {
                    "ranktable.hccl-check.local/source-configmap": "job-summary-train"
                },
            },
            "data": {"hccl.json": payload},
        }
        adapter = ConfigMapRankTableAdapter(FakeKubernetes(document), "training")
        raw, digest = adapter.resolve("train", ("node-a", "node-b"))
        self.assertEqual(raw, payload.encode())
        self.assertEqual(len(digest), 64)

    def test_ranktable_rejects_unowned_configmap(self) -> None:
        adapter = ConfigMapRankTableAdapter(FakeKubernetes({"metadata": {}}), "training")
        with self.assertRaisesRegex(RankTableError, "ownership"):
            adapter.resolve("train", ("node-a",))

    def test_checkpoint_requires_identical_worker_views(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = {"status": "PASS", "iteration": 42, "snapshotSha256": "a" * 64}
            for worker in ("node-a", "node-b"):
                (root / f"{worker}.json").write_text(json.dumps(evidence), encoding="utf-8")
            adapter = CheckpointEvidenceAdapter(root)
            self.assertEqual(adapter.committed_iteration(("node-a", "node-b")), 42)
            evidence["snapshotSha256"] = "b" * 64
            (root / "node-b.json").write_text(json.dumps(evidence), encoding="utf-8")
            self.assertFalse(adapter.views_are_consistent(("node-a", "node-b")))

    def test_ray_submit_is_idempotent_after_uncertain_response(self) -> None:
        client = FakeRayClient("http://ray")
        client.submit_error = TimeoutError("response lost")
        adapter = RayJobsAdapter(lambda address: client)
        returned = adapter.submit_once("http://ray", "run-a00", ("python", "train.py"))
        self.assertEqual(returned, "run-a00")
        self.assertEqual(client.submissions[0]["entrypoint"], "python train.py")


if __name__ == "__main__":
    unittest.main()

