"""Offline portability checks; cluster mutation is intentionally out of scope."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import importlib.util
from pathlib import Path
import shutil
import sys


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str
    required: bool = True


def run_checks(project_root: Path) -> list[Check]:
    checks = [
        Check("python", sys.version_info >= (3, 10), sys.version.split()[0]),
        Check("pyyaml", importlib.util.find_spec("yaml") is not None, "Python module yaml"),
        Check("kubectl", shutil.which("kubectl") is not None, shutil.which("kubectl") or "not found"),
        Check("helm", shutil.which("helm") is not None, shutil.which("helm") or "not found", False),
        Check("legacy-cli", (project_root / "bin/kcc_ray").is_file(), str(project_root / "bin/kcc_ray")),
        Check("cluster-config", (project_root / "config/cluster.yaml").is_file(), str(project_root / "config/cluster.yaml")),
    ]
    return checks


def as_jsonable(checks: list[Check]) -> list[dict[str, object]]:
    return [asdict(check) for check in checks]


def successful(checks: list[Check]) -> bool:
    return all(check.ok for check in checks if check.required)

