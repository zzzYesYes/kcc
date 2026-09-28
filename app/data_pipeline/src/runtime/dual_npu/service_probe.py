"""Probe one loopback MinerU service from its Ray worker node."""

from __future__ import annotations

import argparse
import json
import os
import socket
import urllib.request

import ray


@ray.remote(num_cpus=0.1, resources={"NPU": 1, "MINERU_NPU": 1})
def probe(url: str, service: str) -> dict:
    with urllib.request.urlopen(f"{url}/health", timeout=10) as response:
        body = response.read().decode(errors="replace")
    return {
        "service": service,
        "url": url,
        "status_code": response.status,
        "body": body,
        "worker_ip": socket.gethostbyname(socket.gethostname()),
        "pid": os.getpid(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service", choices=("A", "B"), required=True)
    args = parser.parse_args()
    url = "http://127.0.0.1:30001" if args.service == "A" else "http://127.0.0.1:30002"
    ray.init(address="auto", log_to_driver=False)
    print(json.dumps(ray.get(probe.remote(url, args.service)), ensure_ascii=False))


if __name__ == "__main__":
    main()
