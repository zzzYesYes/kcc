#!/usr/bin/env python3
"""Delete only terminal, unowned cache Pods after a stopped XR converges."""
import json
import ssl
import urllib.parse
import urllib.request

NAMESPACE = "model-serving"
DEPLOYMENT = "qwen38-27b"
API = "https://kubernetes.default.svc"

def request(method, path, body=None):
    token = open("/var/run/secrets/kubernetes.io/serviceaccount/token", encoding="utf-8").read().strip()
    context = ssl.create_default_context(cafile="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(API + path, data=data, method=method, headers={
        "Accept": "application/json", "Authorization": f"Bearer {token}",
        **({} if data is None else {"Content-Type": "application/json"}),
    })
    with urllib.request.urlopen(req, context=context, timeout=15) as response:
        raw = response.read()
        return None if not raw else json.loads(raw)

def is_safe_orphan(pod, revision, claim_name):
    metadata = pod.get("metadata", {})
    labels = metadata.get("labels", {})
    if labels.get("app.kubernetes.io/component") != "model-cache":
        return False
    if labels.get("platform.example.com/deployment") != DEPLOYMENT:
        return False
    if labels.get("platform.example.com/cache-revision") != revision:
        return False
    if metadata.get("ownerReferences"):
        return False
    if metadata.get("deletionTimestamp"):
        return False
    if pod.get("status", {}).get("phase") not in {"Succeeded", "Failed"}:
        return False
    claims = {
        volume.get("persistentVolumeClaim", {}).get("claimName")
        for volume in pod.get("spec", {}).get("volumes", [])
        if volume.get("persistentVolumeClaim")
    }
    return claim_name in claims and bool(metadata.get("name")) and bool(metadata.get("uid"))


def reap(api_request=request):
    xr = api_request("GET", f"/apis/platform.example.com/v1alpha1/namespaces/{NAMESPACE}/modeldeployments/{DEPLOYMENT}")
    conditions = {item.get("type"): item.get("status") for item in xr.get("status", {}).get("conditions", [])}
    if xr.get("spec", {}).get("desiredState") != "Stopped" or conditions.get("Synced") != "True" or conditions.get("Ready") != "True":
        return "cache_reaper=SKIPPED xr_not_converged_stopped"

    revision = xr.get("spec", {}).get("cache", {}).get("revision")
    if not revision:
        raise RuntimeError("cache_reaper=FAIL cache revision is absent")
    workload_selector = urllib.parse.urlencode({"labelSelector": f"platform.example.com/deployment={DEPLOYMENT}"})
    for path in (
        f"/apis/ray.io/v1/namespaces/{NAMESPACE}/rayservices?{workload_selector}",
        f"/apis/ray.io/v1/namespaces/{NAMESPACE}/rayclusters?{workload_selector}",
    ):
        if api_request("GET", path).get("items", []):
            return "cache_reaper=SKIPPED runtime_resources_remain"
    jobs = api_request("GET", f"/apis/batch/v1/namespaces/{NAMESPACE}/jobs?{workload_selector}").get("items", [])
    if any(job.get("status", {}).get("active", 0) for job in jobs):
        return "cache_reaper=SKIPPED active_cache_job_remains"

    selector = urllib.parse.urlencode({"labelSelector": f"platform.example.com/deployment={DEPLOYMENT},platform.example.com/cache-revision={revision}"})
    pods = api_request("GET", f"/api/v1/namespaces/{NAMESPACE}/pods?{selector}").get("items", [])
    claim_name = f"{DEPLOYMENT}-cache"
    deleted = []
    for pod in pods:
        if not is_safe_orphan(pod, revision, claim_name):
            continue
        metadata = pod["metadata"]
        api_request(
            "DELETE",
            f"/api/v1/namespaces/{NAMESPACE}/pods/{metadata['name']}",
            {"preconditions": {"uid": metadata["uid"]}},
        )
        deleted.append(metadata["name"])
    return "cache_reaper=PASS deleted=" + ",".join(deleted or ["none"])


if __name__ == "__main__":
    print(reap())
