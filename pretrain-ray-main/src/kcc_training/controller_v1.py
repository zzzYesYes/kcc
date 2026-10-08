"""Stable v1 controller process used by the authoritative Helm chart."""

from __future__ import annotations

from typing import Sequence

from . import controller_release
from .ray_jobs_v1 import V1RayJobsRest


def main(argv: Sequence[str] | None = None) -> int:
    controller_release.ReleaseRayJobsRest = V1RayJobsRest
    return controller_release.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
