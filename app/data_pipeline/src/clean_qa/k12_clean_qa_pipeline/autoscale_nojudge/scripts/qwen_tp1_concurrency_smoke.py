#!/usr/bin/env python3
"""Exercise one Qwen TP1 endpoint at its configured max sequence count."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import time
import urllib.request


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--max-tokens", type=int, default=256)
    args = parser.parse_args()

    passage = (
        "分数表示把一个整体平均分成若干份，其中的一份或几份。"
        "分母表示平均分成的份数，分子表示取出的份数。"
    ) * 80

    def generate(index: int) -> dict[str, object]:
        body = json.dumps(
            {
                "model": "qwen3.6-35b-a3b",
                "messages": [
                    {
                        "role": "user",
                        "content": (
                            "根据教材片段生成一个简洁、事实准确的问题和答案："
                            f"{passage}\n请求编号：{index}"
                        ),
                    }
                ],
                "temperature": 0,
                "max_tokens": args.max_tokens,
            }
        ).encode()
        request = urllib.request.Request(
            f"{args.url.rstrip('/')}/v1/chat/completions",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        started = time.time()
        with urllib.request.urlopen(request, timeout=300) as response:
            payload = json.loads(response.read())
        usage = payload.get("usage", {})
        return {
            "index": index,
            "seconds": round(time.time() - started, 3),
            "finish_reason": payload["choices"][0]["finish_reason"],
            "prompt_tokens": usage.get("prompt_tokens"),
            "completion_tokens": usage.get("completion_tokens"),
        }

    started = time.time()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.concurrency
    ) as executor:
        results = list(executor.map(generate, range(args.concurrency)))
    print(
        json.dumps(
            {
                "concurrency": args.concurrency,
                "wall_seconds": round(time.time() - started, 3),
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
