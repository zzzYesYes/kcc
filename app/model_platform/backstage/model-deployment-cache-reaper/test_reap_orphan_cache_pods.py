import importlib.util
import pathlib
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("reap_orphan_cache_pods.py")
SPEC = importlib.util.spec_from_file_location("cache_reaper", MODULE_PATH)
cache_reaper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cache_reaper)


def pod(*, phase="Succeeded", owners=None, revision="r1", claim="qwen38-27b-cache"):
    return {
        "metadata": {
            "name": "qwen38-cache-abc",
            "uid": "pod-uid",
            "labels": {
                "app.kubernetes.io/component": "model-cache",
                "platform.example.com/deployment": "qwen38-27b",
                "platform.example.com/cache-revision": revision,
            },
            "ownerReferences": owners or [],
        },
        "spec": {"volumes": [{"name": "cache", "persistentVolumeClaim": {"claimName": claim}}]},
        "status": {"phase": phase},
    }


class CacheReaperTest(unittest.TestCase):
    def test_accepts_only_terminal_unowned_pod_for_exact_revision_and_claim(self):
        self.assertTrue(cache_reaper.is_safe_orphan(pod(), "r1", "qwen38-27b-cache"))
        self.assertFalse(cache_reaper.is_safe_orphan(pod(phase="Running"), "r1", "qwen38-27b-cache"))
        self.assertFalse(cache_reaper.is_safe_orphan(pod(owners=[{"uid": "job-uid"}]), "r1", "qwen38-27b-cache"))
        self.assertFalse(cache_reaper.is_safe_orphan(pod(revision="old"), "r1", "qwen38-27b-cache"))
        self.assertFalse(cache_reaper.is_safe_orphan(pod(claim="other-cache"), "r1", "qwen38-27b-cache"))

    def test_reap_deletes_by_uid_after_all_stop_guards_pass(self):
        calls = []

        def api_request(method, path, body=None):
            calls.append((method, path, body))
            if "modeldeployments/qwen38-27b" in path:
                return {
                    "spec": {"desiredState": "Stopped", "cache": {"revision": "r1"}},
                    "status": {"conditions": [{"type": "Synced", "status": "True"}, {"type": "Ready", "status": "True"}]},
                }
            if "/pods?" in path:
                return {"items": [pod()]}
            return {"items": []}

        self.assertEqual(cache_reaper.reap(api_request), "cache_reaper=PASS deleted=qwen38-cache-abc")
        self.assertIn(
            ("DELETE", "/api/v1/namespaces/model-serving/pods/qwen38-cache-abc", {"preconditions": {"uid": "pod-uid"}}),
            calls,
        )

    def test_reap_does_not_delete_before_stopped_convergence(self):
        def api_request(method, path, body=None):
            return {
                "spec": {"desiredState": "Stopped", "cache": {"revision": "r1"}},
                "status": {"conditions": [{"type": "Synced", "status": "False"}, {"type": "Ready", "status": "True"}]},
            }

        self.assertEqual(cache_reaper.reap(api_request), "cache_reaper=SKIPPED xr_not_converged_stopped")


if __name__ == "__main__":
    unittest.main()
