"""Launchpad defaults for the standalone lifecycle demonstration."""

import os


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))

LIFECYCLE_CONFIG = {
    "ops": {
        "control_qwen_worker": {
            "config": {
                "action": "status",
                "startup_timeout_seconds": _env_int(
                    "QWEN_STARTUP_TIMEOUT_SECONDS", 1800
                ),
                "shutdown_timeout_seconds": _env_int(
                    "QWEN_SHUTDOWN_TIMEOUT_SECONDS", 300
                ),
                "poll_interval_seconds": _env_float(
                    "QWEN_POLL_INTERVAL_SECONDS", 5.0
                ),
            }
        }
    }
}

LIFECYCLE_8NPU_CONFIG = {
    "ops": {
        "control_qwen_8npu_worker": {
            "config": {
                "action": "status",
                "startup_timeout_seconds": _env_int(
                    "QWEN8_STARTUP_TIMEOUT_SECONDS", 1800
                ),
                "shutdown_timeout_seconds": _env_int(
                    "QWEN8_SHUTDOWN_TIMEOUT_SECONDS", 300
                ),
                "poll_interval_seconds": _env_float(
                    "QWEN8_POLL_INTERVAL_SECONDS", 5.0
                ),
            }
        }
    }
}

CHAT_CONFIG = {
    "ops": {
        "call_qwen_chat": {
            "config": {
                "target": "standard",
                "prompt": "请用一句中文介绍你自己。",
                "system_prompt": "You are a precise and helpful assistant.",
                "history_json": "[]",
                "temperature": 0.0,
                "max_tokens": 256,
                "enable_thinking": False,
                "timeout_seconds": _env_int("QWEN_CHAT_TIMEOUT_SECONDS", 300),
            }
        }
    }
}
