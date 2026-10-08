import json
import unittest

from kcc_training.kube_api import ApiResponse, KubernetesApi, KubernetesOwnershipError, LeaseLock


class RecordedTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, path, body, headers):
        self.calls.append((method, path, body, headers))
        return self.responses.pop(0)


def response(status, value=None):
    body = b"" if value is None else json.dumps(value).encode()
    return ApiResponse(status, body)


class KubernetesApiTests(unittest.TestCase):
    def test_delete_uses_uid_precondition_in_single_request(self) -> None:
        transport = RecordedTransport([response(200, {"kind": "Status"})])
        api = KubernetesApi(transport=transport)
        api.delete_owned("/apis/ray.io/v1/namespaces/train/rayclusters/run-a00", "uid-1")
        method, path, body, _headers = transport.calls[0]
        self.assertEqual(method, "DELETE")
        self.assertEqual(path, "/apis/ray.io/v1/namespaces/train/rayclusters/run-a00")
        self.assertEqual(json.loads(body)["preconditions"], {"uid": "uid-1"})

    def test_upsert_replaces_with_current_resource_version(self) -> None:
        current = {"metadata": {"name": "x", "resourceVersion": "7"}}
        replaced = {"metadata": {"name": "x", "resourceVersion": "8"}}
        transport = RecordedTransport([response(200, current), response(200, replaced)])
        api = KubernetesApi(transport=transport)
        result = api.upsert("/objects", "/objects/x", {"metadata": {"name": "x"}})
        self.assertEqual(result["metadata"]["resourceVersion"], "8")
        self.assertEqual(json.loads(transport.calls[1][2])["metadata"]["resourceVersion"], "7")

    def test_upsert_refuses_another_training_run_owner(self) -> None:
        current = {"metadata": {
            "name": "x", "resourceVersion": "7",
            "annotations": {"training.kcc.io/run-uid": "old-run"},
            "ownerReferences": [{"controller": True, "uid": "old-run"}],
        }}
        desired = {"metadata": {
            "name": "x",
            "annotations": {"training.kcc.io/run-uid": "new-run"},
            "ownerReferences": [{"controller": True, "uid": "new-run"}],
        }}
        api = KubernetesApi(transport=RecordedTransport([response(200, current)]))
        with self.assertRaises(KubernetesOwnershipError):
            api.upsert("/objects", "/objects/x", desired)

    def test_upsert_refuses_mismatched_controller_owner_uid(self) -> None:
        current = {"metadata": {
            "name": "x", "resourceVersion": "7",
            "annotations": {"training.kcc.io/run-uid": "same-run"},
            "ownerReferences": [{"controller": True, "uid": "old-run"}],
        }}
        desired = {"metadata": {
            "name": "x",
            "annotations": {"training.kcc.io/run-uid": "same-run"},
            "ownerReferences": [{"controller": True, "uid": "same-run"}],
        }}
        api = KubernetesApi(transport=RecordedTransport([response(200, current)]))
        with self.assertRaises(KubernetesOwnershipError):
            api.upsert("/objects", "/objects/x", desired)

    def test_lease_does_not_steal_unexpired_holder(self) -> None:
        lease = {
            "metadata": {"name": "controller", "resourceVersion": "3"},
            "spec": {
                "holderIdentity": "other",
                "renewTime": "2026-08-19T00:00:00Z",
                "leaseDurationSeconds": 30,
                "leaseTransitions": 0,
            },
        }
        transport = RecordedTransport([response(200, lease)])
        lock = LeaseLock(
            KubernetesApi(transport=transport),
            namespace="train",
            name="controller",
            identity="me",
            clock=lambda: 1787097610,
        )
        self.assertFalse(lock.acquire_or_renew())
        self.assertEqual(len(transport.calls), 1)

    def test_lease_takes_expired_holder_with_resource_version(self) -> None:
        lease = {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": "controller", "resourceVersion": "3"},
            "spec": {
                "holderIdentity": "other",
                "renewTime": "2026-08-19T00:00:00Z",
                "leaseDurationSeconds": 30,
                "leaseTransitions": 1,
            },
        }
        transport = RecordedTransport([response(200, lease), response(200, lease)])
        lock = LeaseLock(
            KubernetesApi(transport=transport),
            namespace="train",
            name="controller",
            identity="me",
            clock=lambda: 1787097700,
        )
        self.assertTrue(lock.acquire_or_renew())
        replaced = json.loads(transport.calls[1][2])
        self.assertEqual(replaced["metadata"]["resourceVersion"], "3")
        self.assertEqual(replaced["spec"]["leaseTransitions"], 2)
        self.assertTrue(lock.held)
        with lock.maintain():
            self.assertTrue(lock.held)
        self.assertFalse(lock.held)


if __name__ == "__main__":
    unittest.main()
