"""Submit one chat request through the Ray-managed Qwen API resource."""

from __future__ import annotations

import argparse
import base64
import json
import time
from urllib.request import Request, urlopen

import ray


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prompt-b64", required=True)
    parser.add_argument("--system-prompt-b64", required=True)
    parser.add_argument("--history-b64", required=True)
    parser.add_argument("--temperature", type=float, required=True)
    parser.add_argument("--max-tokens", type=int, required=True)
    parser.add_argument("--enable-thinking", choices=("true", "false"), required=True)
    return parser.parse_args()


@ray.remote(num_cpus=0, resources={"QWEN36_A3B_API": 0.001})
def call_qwen(payload: dict) -> dict:
    started = time.monotonic()
    request = Request(
        "http://qwen36-35b-a3b.k12.svc.cluster.local:8000/v1/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=300) as response:
        body = json.loads(response.read().decode())
    return {
        "answer": body["choices"][0]["message"]["content"],
        "usage": body.get("usage", {}),
        "model": body.get("model", payload["model"]),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "ray_node_ip": ray.util.get_node_ip_address(),
    }


def main() -> None:
    args = parse_args()
    decode = lambda value: base64.b64decode(value).decode()
    payload = {
        "model": "qwen3.6-35b-a3b",
        "messages": [
            {"role": "system", "content": decode(args.system_prompt_b64)},
            *json.loads(decode(args.history_b64)),
            {"role": "user", "content": decode(args.prompt_b64)},
        ],
        "temperature": args.temperature,
        "max_tokens": args.max_tokens,
        "chat_template_kwargs": {"enable_thinking": args.enable_thinking == "true"},
    }
    ray.init(address="auto")
    result = ray.get(call_qwen.remote(payload))
    print("QWEN_CHAT_RESULT " + json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
