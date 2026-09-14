"""Replaceable infrastructure adapters shipped with the control-plane package."""

from .checkpoint import CheckpointEvidenceAdapter, CheckpointEvidenceError
from .command import CommandError, CommandResult, SubprocessRunner
from .kubernetes import KubernetesCli, KubernetesError
from .ranktable import ConfigMapRankTableAdapter, RankTableError
from .ray_jobs import RayJobsAdapter, RayJobsError

__all__ = [
    "CheckpointEvidenceAdapter",
    "CheckpointEvidenceError",
    "CommandError",
    "CommandResult",
    "ConfigMapRankTableAdapter",
    "KubernetesCli",
    "KubernetesError",
    "RankTableError",
    "RayJobsAdapter",
    "RayJobsError",
    "SubprocessRunner",
]

