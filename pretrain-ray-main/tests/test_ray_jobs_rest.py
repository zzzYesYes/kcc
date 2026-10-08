import json
import unittest

from kcc_training.ray_jobs_rest import RayJobsRest, RayJobsRestError, RestResponse


class Transport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, url, body):
        self.calls.append((method, url, body))
        return self.responses.pop(0)


def response(status, value):
    return RestResponse(status, json.dumps(value).encode())


class RayJobsRestTests(unittest.TestCase):
    def test_submit_uses_fixed_runtime_entrypoint_and_command_json(self) -> None:
        transport = Transport([response(404, {}), response(200, {"submission_id": "run-a00"})])
        jobs = RayJobsRest(transport)
        jobs.submit_once("http://ray", "run-a00", ("python", "train.py"), metadata={"runUid": "u1"})
        payload = json.loads(transport.calls[1][2])
        self.assertEqual(payload["submission_id"], "run-a00")
        self.assertEqual(json.loads(payload["runtime_env"]["env_vars"]["KCC_COMMAND_JSON"]), ["python", "train.py"])

    def test_existing_job_requires_matching_ownership(self) -> None:
        transport = Transport([response(200, {"submission_id": "run-a00", "metadata": {"runUid": "other"}})])
        with self.assertRaisesRegex(RayJobsRestError, "ownership"):
            RayJobsRest(transport).submit_once("http://ray", "run-a00", ("python",), metadata={"runUid": "u1"})

    def test_failed_submit_reconnects_same_submission(self) -> None:
        transport = Transport([
            response(404, {}), response(503, {}),
            response(200, {"submission_id": "run-a00", "metadata": {"runUid": "u1"}}),
        ])
        returned = RayJobsRest(transport).submit_once("http://ray", "run-a00", ("python",), metadata={"runUid": "u1"})
        self.assertEqual(returned, "run-a00")


if __name__ == "__main__":
    unittest.main()
