#!/usr/bin/env python3
"""Inject a verified Ray/HCCL topology into one training-script copy per worker."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import sys
from typing import Any, Mapping, Sequence


RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
RANK_TABLE_PATH = "/user/serverid/devindex/config/hccl.json"
MS_TORCHRUN = "/root/miniconda3/envs/ms/bin/torchrun"
DEFAULT_TRAINING_CWD = "/mnt/models/CODE/MindSpeed-LLM-v2.3.0"
DEFAULT_LOG_ROOT = "/mnt/models/pretrain-ray-platform/log"
DEFAULT_ARCHIVE_ROOT = "/mnt/models/pretrain-ray-platform/archive"


class InjectionError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_regular_file(path: Path, label: str) -> None:
    if not path.is_file() or path.is_symlink():
        raise InjectionError(f"{label} is not a regular file: {path}")


def require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise InjectionError(f"{label} must be an object")
    return value


def load_pass_result(path: Path, label: str) -> Mapping[str, Any]:
    require_regular_file(path, label)
    try:
        wrapper = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise InjectionError(f"cannot parse {label}: {error}") from error
    wrapper = require_mapping(wrapper, label)
    if wrapper.get("status") != "PASS":
        raise InjectionError(f"{label} wrapper is not PASS")
    result = require_mapping(wrapper.get("result"), f"{label} result")
    if result.get("status") != "PASS" or result.get("mode") != "execute":
        raise InjectionError(f"{label} result is not execute/PASS")
    return result


def load_topology(
    hccl_evidence: Path,
    ping_evidence: Path,
) -> tuple[str, tuple[dict[str, Any], ...]]:
    hccl_result = load_pass_result(hccl_evidence, "HCCL evidence")
    preflight = require_mapping(
        hccl_result.get("preflight"),
        "HCCL preflight",
    )
    if preflight.get("status") != "PASS":
        raise InjectionError("HCCL preflight is not PASS")

    ranktable_sha256 = preflight.get("ranktable_sha256")
    if (
        not isinstance(ranktable_sha256, str)
        or SHA256_PATTERN.fullmatch(ranktable_sha256) is None
    ):
        raise InjectionError("HCCL preflight has no valid RankTable SHA256")

    raw_workers = preflight.get("workers")
    if not isinstance(raw_workers, list) or not raw_workers:
        raise InjectionError("HCCL preflight has no workers")
    if preflight.get("server_count") != len(raw_workers):
        raise InjectionError("HCCL server count differs from its worker list")

    workers: list[dict[str, Any]] = []
    for raw_worker in raw_workers:
        worker = dict(require_mapping(raw_worker, "HCCL worker"))
        for field in ("pod_name", "node_name", "server_id", "ray_node_id"):
            if not isinstance(worker.get(field), str) or not worker[field]:
                raise InjectionError(f"HCCL worker has no valid {field}")
        if not isinstance(worker.get("npu_count"), int) or worker["npu_count"] <= 0:
            raise InjectionError("HCCL worker has an invalid npu_count")
        if not isinstance(worker.get("rank_start"), int) or worker["rank_start"] < 0:
            raise InjectionError("HCCL worker has an invalid rank_start")
        workers.append(worker)

    workers.sort(key=lambda item: item["rank_start"])
    npu_counts = {worker["npu_count"] for worker in workers}
    if len(npu_counts) != 1:
        raise InjectionError(
            "workers expose different NPU counts; one torchrun profile cannot use it"
        )
    npus_per_worker = next(iter(npu_counts))
    expected_rank_starts = [
        node_rank * npus_per_worker for node_rank in range(len(workers))
    ]
    if [worker["rank_start"] for worker in workers] != expected_rank_starts:
        raise InjectionError("HCCL worker rank ranges are not contiguous")
    world_size = len(workers) * npus_per_worker
    if preflight.get("world_size") != world_size:
        raise InjectionError("HCCL world size differs from the worker topology")

    ping_result = load_pass_result(ping_evidence, "HCCL ping evidence")
    raw_ping_workers = ping_result.get("workers")
    if (
        ping_result.get("worker_count") != len(workers)
        or not isinstance(raw_ping_workers, list)
        or len(raw_ping_workers) != len(workers)
    ):
        raise InjectionError("HCCL ping worker count differs from HCCL preflight")

    ping_by_pod: dict[str, Mapping[str, Any]] = {}
    for raw_ping_worker in raw_ping_workers:
        ping_worker = require_mapping(raw_ping_worker, "HCCL ping worker")
        pod_name = ping_worker.get("pod")
        if not isinstance(pod_name, str) or not pod_name:
            raise InjectionError("HCCL ping worker has no pod name")
        if pod_name in ping_by_pod:
            raise InjectionError(f"duplicate Pod in HCCL ping evidence: {pod_name}")
        ping_by_pod[pod_name] = ping_worker

    enriched: list[dict[str, Any]] = []
    seen_pods: set[str] = set()
    seen_pod_ips: set[str] = set()
    for node_rank, worker in enumerate(workers):
        pod_name = worker["pod_name"]
        ping_worker = ping_by_pod.get(pod_name)
        if ping_worker is None:
            raise InjectionError(
                f"HCCL worker is absent from ping evidence: {pod_name}"
            )
        if ping_worker.get("node_id") != worker["ray_node_id"]:
            raise InjectionError(
                f"Ray node identity differs between HCCL stages: {pod_name}"
            )
        if ping_worker.get("resource_count") != npus_per_worker:
            raise InjectionError(
                f"Ray NPU resource count differs for worker: {pod_name}"
            )
        pod_ip = ping_worker.get("ray_node_ip")
        try:
            parsed_ip = ipaddress.ip_address(pod_ip)
        except (TypeError, ValueError) as error:
            raise InjectionError(
                f"HCCL ping evidence has invalid Pod IP for {pod_name}: {pod_ip}"
            ) from error
        if parsed_ip.version != 4:
            raise InjectionError(f"only IPv4 Pod IP is supported: {pod_ip}")
        normalized_ip = str(parsed_ip)
        if pod_name in seen_pods or normalized_ip in seen_pod_ips:
            raise InjectionError("HCCL topology contains a duplicate Pod or Pod IP")
        seen_pods.add(pod_name)
        seen_pod_ips.add(normalized_ip)
        worker["node_rank"] = node_rank
        worker["pod_ip"] = normalized_ip
        enriched.append(worker)

    if set(ping_by_pod) != seen_pods:
        raise InjectionError("HCCL ping evidence contains unexpected workers")
    return ranktable_sha256, tuple(enriched)


def replace_once(
    text: str,
    pattern: str,
    replacement: str,
    label: str,
) -> str:
    updated, count = re.subn(
        pattern,
        lambda _match: replacement,
        text,
        flags=re.MULTILINE,
    )
    if count != 1:
        raise InjectionError(
            f"{label}: expected exactly one active assignment, found {count}"
        )
    return updated


def source_master_port(source: str) -> int:
    matches = re.findall(
        r"^[ \t]*(?:export[ \t]+)?MASTER_PORT=([0-9]+)"
        r"[ \t]*(?:#.*)?$",
        source,
        flags=re.MULTILINE,
    )
    if len(matches) != 1:
        raise InjectionError(
            f"MASTER_PORT: expected exactly one active assignment, found {len(matches)}"
        )
    port = int(matches[0])
    if not 1024 <= port <= 65535:
        raise InjectionError(f"MASTER_PORT is outside 1024..65535: {port}")
    return port


def source_positive_int(source: str, name: str) -> int:
    matches = re.findall(
        rf"^[ \t]*(?:export[ \t]+)?{re.escape(name)}=([0-9]+)"
        r"[ \t]*(?:#.*)?$",
        source,
        flags=re.MULTILINE,
    )
    if len(matches) != 1:
        raise InjectionError(
            f"{name}: expected exactly one active assignment, found {len(matches)}"
        )
    value = int(matches[0])
    if value <= 0:
        raise InjectionError(f"{name} must be positive")
    return value


def source_checkpoint_save_dir(source: str) -> str | None:
    active_save = re.search(
        r"^[ \t]*--save(?:[ \t=]|$)",
        source,
        flags=re.MULTILINE,
    )
    if active_save is None:
        return None
    matches = re.findall(
        r"^[ \t]*(?:export[ \t]+)?CKPT_SAVE_DIR="
        r'(?:\"([^\"]+)\"|\'([^\']+)\'|([^ \t#]+))'
        r"[ \t]*(?:#.*)?$",
        source,
        flags=re.MULTILINE,
    )
    if len(matches) != 1:
        raise InjectionError(
            "active --save requires one literal CKPT_SAVE_DIR assignment"
        )
    value = next(part for part in matches[0] if part)
    if not value.startswith("/"):
        raise InjectionError("CKPT_SAVE_DIR must be an absolute path")
    return value


def source_checkpoint_load_dir(source: str) -> str | None:
    active_loads = re.findall(
        r"^[ \t]*--load(?:[ \t=]+)([^ \t\\#]+)",
        source,
        flags=re.MULTILINE,
    )
    if not active_loads:
        return None
    if len(active_loads) != 1:
        raise InjectionError(
            f"--load: expected at most one active option, found {len(active_loads)}"
        )
    matches = re.findall(
        r"^[ \t]*(?:export[ \t]+)?CKPT_LOAD_DIR="
        r'(?:\"([^\"]+)\"|\'([^\']+)\'|([^ \t#]+))'
        r"[ \t]*(?:#.*)?$",
        source,
        flags=re.MULTILINE,
    )
    if len(matches) != 1:
        raise InjectionError(
            "active --load requires one literal CKPT_LOAD_DIR assignment"
        )
    value = next(part for part in matches[0] if part)
    if not value.startswith("/"):
        raise InjectionError("CKPT_LOAD_DIR must be an absolute path")
    load_token = active_loads[0]
    accepted_tokens = {
        "$CKPT_LOAD_DIR",
        "${CKPT_LOAD_DIR}",
        value,
        f'"{value}"',
        f"'{value}'",
    }
    if load_token not in accepted_tokens:
        raise InjectionError(
            "active --load does not reference the declared CKPT_LOAD_DIR"
        )
    return value


def source_has_option(source: str, option: str) -> bool:
    return re.search(
        rf"^[ \t]*{re.escape(option)}(?:[ \t=]|\\|$)",
        source,
        flags=re.MULTILINE,
    ) is not None


def remove_active_option(text: str, option: str) -> str:
    pattern = rf"^[ \t]*{re.escape(option)}(?:[ \t=].*)?(?:\n|$)"
    updated, count = re.subn(pattern, "", text, flags=re.MULTILINE)
    if count > 1:
        raise InjectionError(
            f"{option}: expected at most one active option, found {count}"
        )
    return updated


def replace_optional_option_value(
    text: str,
    *,
    option: str,
    value: str,
) -> str:
    pattern = rf"^([ \t]*){re.escape(option)}(?:[ \t=]+)[^ \t\\#]+([ \t]+\\)?[ \t]*$"
    updated, count = re.subn(
        pattern,
        lambda match: f"{match.group(1)}{option} {value}{match.group(2) or ''}",
        text,
        flags=re.MULTILINE,
    )
    if count > 1:
        raise InjectionError(
            f"{option}: expected at most one active option, found {count}"
        )
    return updated


def render_script(
    source: str,
    *,
    run_id: str,
    master_addr: str,
    master_port: int,
    workers: int,
    npus_per_worker: int,
    node_rank: int,
    fresh_start: bool = False,
) -> tuple[str, str]:
    if fresh_start:
        archive_root = f"{DEFAULT_ARCHIVE_ROOT}/{run_id}"
        log_root = f"{archive_root}/logs"
    else:
        archive_root = None
        log_root = f"{DEFAULT_LOG_ROOT}/{run_id}/logs"
    log_file = f"{log_root}/node-rank-{node_rank}.log"
    text = source
    if fresh_start:
        checkpoint_dir = f"{archive_root}/checkpoints"
        text = replace_once(
            text,
            r"^[ \t]*(?:export[ \t]+)?CKPT_SAVE_DIR=.*$",
            f'CKPT_SAVE_DIR="{checkpoint_dir}"',
            "CKPT_SAVE_DIR",
        )
        if source_checkpoint_load_dir(source) is not None:
            text = replace_once(
                text,
                r"^[ \t]*(?:export[ \t]+)?CKPT_LOAD_DIR=.*$",
                f'CKPT_LOAD_DIR="{checkpoint_dir}"',
                "CKPT_LOAD_DIR",
            )
        text = remove_active_option(text, "--load")
        text = remove_active_option(text, "--exit-on-missing-checkpoint")
        text = replace_optional_option_value(
            text,
            option="--wandb-save-dir",
            value=f"{log_root}/wandb",
        )
        text = replace_optional_option_value(
            text,
            option="--tensorboard-dir",
            value=f"{log_root}/tensorboard",
        )
    replacements = (
        (
            r"^[ \t]*export[ \t]+RANK_TABLE_FILE=.*$",
            f"export RANK_TABLE_FILE={RANK_TABLE_PATH}",
            "RANK_TABLE_FILE",
        ),
        (
            r"^[ \t]*NPUS_PER_NODE=.*$",
            f"NPUS_PER_NODE={npus_per_worker}",
            "NPUS_PER_NODE",
        ),
        (
            r"^[ \t]*MASTER_ADDR=.*$",
            f"MASTER_ADDR={master_addr}",
            "MASTER_ADDR",
        ),
        (
            r"^[ \t]*MASTER_PORT=.*$",
            f"MASTER_PORT={master_port}",
            "MASTER_PORT",
        ),
        (
            r"^[ \t]*NNODES=.*$",
            f"NNODES={workers}",
            "NNODES",
        ),
        (
            r"^[ \t]*NODE_RANK=.*$",
            f"NODE_RANK={node_rank}",
            "NODE_RANK",
        ),
        (
            r"^[ \t]*LOG_FILE=.*$",
            (
                f'LOG_FILE="{log_file}"\n'
                'mkdir -p -- "$(dirname -- "$LOG_FILE")"'
            ),
            "LOG_FILE",
        ),
    )
    for pattern, replacement, label in replacements:
        text = replace_once(text, pattern, replacement, label)

    torchrun_pattern = re.compile(
        r"^([ \t]*)torchrun([ \t]+.*)$",
        flags=re.MULTILINE,
    )
    text, count = torchrun_pattern.subn(
        lambda match: f"{match.group(1)}{MS_TORCHRUN}{match.group(2)}",
        text,
    )
    if count != 1:
        raise InjectionError(
            f"torchrun: expected exactly one active command, found {count}"
        )
    return text, log_file


def create_injection(
    *,
    source_path: Path,
    hccl_evidence: Path,
    ping_evidence: Path,
    output_dir: Path,
    run_id: str,
    training_cwd: str,
    master_port: int | None,
    allow_topology_change: bool,
    confirm_checkpoint_exclusive: bool = False,
    require_resumable_checkpoint: bool = False,
    fresh_start: bool = False,
) -> Path:
    # Accepted only for callers using the former API; it no longer gates writes.
    del confirm_checkpoint_exclusive
    if RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise InjectionError("run ID contains unsupported characters")
    if not training_cwd.startswith("/"):
        raise InjectionError("training cwd must be an absolute worker path")
    require_regular_file(source_path, "training source")
    source_sha256 = sha256_file(source_path)
    try:
        source = source_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise InjectionError(f"cannot read training source: {error}") from error
    checkpoint_save_dir = source_checkpoint_save_dir(source)
    checkpoint_load_dir = source_checkpoint_load_dir(source)
    if fresh_start and require_resumable_checkpoint:
        raise InjectionError(
            "fresh start cannot require an existing resumable checkpoint"
        )
    if fresh_start and checkpoint_save_dir is None:
        raise InjectionError(
            "fresh start requires one active --save and CKPT_SAVE_DIR"
        )
    if require_resumable_checkpoint:
        if checkpoint_load_dir is None:
            raise InjectionError(
                "recovery requires one active --load and a literal CKPT_LOAD_DIR"
            )
        if checkpoint_save_dir != checkpoint_load_dir:
            raise InjectionError(
                "recovery requires CKPT_LOAD_DIR and CKPT_SAVE_DIR to be identical"
            )
        unsupported = [
            option
            for option in (
                "--finetune",
                "--no-save-optim",
                "--no-save-rng",
                "--no-load-optim",
                "--no-load-rng",
                "--use-dist-ckpt",
                "--auto-detect-ckpt-format",
            )
            if source_has_option(source, option)
        ]
        if unsupported:
            raise InjectionError(
                "strict recovery does not support checkpoint option(s): "
                + ",".join(unsupported)
            )
        format_matches = re.findall(
            r"^[ \t]*--ckpt-format(?:[ \t=]+)([^ \t\\#]+)",
            source,
            flags=re.MULTILINE,
        )
        if len(format_matches) > 1 or (
            format_matches and format_matches[0] != "torch"
        ):
            raise InjectionError(
                "strict recovery currently supports the deployed legacy torch "
                "checkpoint format only"
            )

    selected_master_port = (
        source_master_port(source) if master_port is None else master_port
    )
    if not 1024 <= selected_master_port <= 65535:
        raise InjectionError("master port must be within 1024..65535")
    ranktable_sha256, topology_workers = load_topology(
        hccl_evidence,
        ping_evidence,
    )
    worker_count = len(topology_workers)
    npus_per_worker = int(topology_workers[0]["npu_count"])
    master_addr = str(topology_workers[0]["pod_ip"])
    source_workers = source_positive_int(source, "NNODES")
    source_npus_per_worker = source_positive_int(source, "NPUS_PER_NODE")
    topology_changed = (
        source_workers != worker_count
        or source_npus_per_worker != npus_per_worker
    )
    if topology_changed and not allow_topology_change:
        raise InjectionError(
            "discovered topology differs from the source script "
            f"({source_workers}x{source_npus_per_worker} -> "
            f"{worker_count}x{npus_per_worker}); "
            "checkpoint compatibility must be reviewed before explicit "
            "--allow-topology-change approval"
        )

    archive_root = (
        f"{DEFAULT_ARCHIVE_ROOT}/{run_id}" if fresh_start else None
    )
    runtime_log_root = (
        f"{archive_root}/logs"
        if archive_root is not None
        else f"{DEFAULT_LOG_ROOT}/{run_id}"
    )
    effective_checkpoint_save_dir = (
        f"{archive_root}/checkpoints"
        if archive_root is not None
        else checkpoint_save_dir
    )
    effective_checkpoint_load_dir = (
        None if fresh_start else checkpoint_load_dir
    )

    try:
        output_dir.mkdir(parents=True, exist_ok=False)
        scripts_dir = output_dir / "scripts"
        scripts_dir.mkdir()
    except OSError as error:
        raise InjectionError(
            f"cannot create new injection directory {output_dir}: {error}"
        ) from error

    nodes: list[dict[str, Any]] = []
    for worker in topology_workers:
        node_rank = int(worker["node_rank"])
        rendered, log_file = render_script(
            source,
            run_id=run_id,
            master_addr=master_addr,
            master_port=selected_master_port,
            workers=worker_count,
            npus_per_worker=npus_per_worker,
            node_rank=node_rank,
            fresh_start=fresh_start,
        )
        script_name = f"train-node-rank-{node_rank}.sh"
        script_path = scripts_dir / script_name
        script_path.write_text(rendered, encoding="utf-8")
        script_path.chmod(0o750)
        nodes.append(
            {
                "nodeRank": node_rank,
                "rankStart": worker["rank_start"],
                "serverId": worker["server_id"],
                "nodeName": worker["node_name"],
                "podName": worker["pod_name"],
                "podIp": worker["pod_ip"],
                "rayNodeId": worker["ray_node_id"],
                "script": script_name,
                "scriptSha256": sha256_file(script_path),
                "logFile": log_file,
            }
        )

    if sha256_file(source_path) != source_sha256:
        raise InjectionError("training source changed while parameters were injected")
    manifest = {
        "schemaVersion": "training-injection/v1",
        "runId": run_id,
        "launchMode": "fresh" if fresh_start else "resume",
        "source": {
            "path": str(source_path),
            "sha256": source_sha256,
            "modified": False,
            "hcclEvidence": str(hccl_evidence),
            "hcclEvidenceSha256": sha256_file(hccl_evidence),
            "pingEvidence": str(ping_evidence),
            "pingEvidenceSha256": sha256_file(ping_evidence),
        },
        "topology": {
            "workers": worker_count,
            "npusPerWorker": npus_per_worker,
            "worldSize": worker_count * npus_per_worker,
            "masterAddr": master_addr,
            "masterPort": selected_master_port,
            "rankTablePath": RANK_TABLE_PATH,
            "rankTableSha256": ranktable_sha256,
        },
        "topologyChange": {
            "changed": topology_changed,
            "explicitlyApproved": allow_topology_change,
            "sourceWorkers": source_workers,
            "sourceNpusPerWorker": source_npus_per_worker,
        },
        "runtime": {
            "trainingCwd": training_cwd,
            "torchrun": MS_TORCHRUN,
            "shell": "/bin/bash -o pipefail",
            "logRoot": runtime_log_root,
            "archiveRoot": archive_root,
        },
        "preserved": {
            "checkpoint": True,
            "checkpointDeletion": False,
            "trainingIterations": True,
            "dataArguments": True,
            "wandbArguments": True,
        },
        "checkpointWrite": {
            "enabled": effective_checkpoint_save_dir is not None,
            "saveDir": effective_checkpoint_save_dir,
            # Retained as a constant only for training-injection/v1 readers.
            "exclusiveConfirmed": False,
        },
        "checkpointLoad": {
            "enabled": effective_checkpoint_load_dir is not None,
            "loadDir": effective_checkpoint_load_dir,
            "trackerFilename": "latest_checkpointed_iteration.txt",
            "selection": "megatron-tracker",
            "requiredForRecovery": require_resumable_checkpoint,
            "format": "torch",
            "tensorParallelSize": source_positive_int(source, "TP"),
            "pipelineParallelSize": source_positive_int(source, "PP"),
            "distributedOptimizer": source_has_option(
                source, "--use-distributed-optimizer"
            ),
        },
        "nodes": nodes,
    }
    manifest_path = output_dir / "injection.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest_path


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--hccl-evidence", type=Path, required=True)
    parser.add_argument("--hccl-ping-evidence", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--training-cwd", default=DEFAULT_TRAINING_CWD)
    parser.add_argument("--master-port", type=int)
    parser.add_argument(
        "--allow-topology-change",
        action="store_true",
        help="explicitly approve changing source NNODES/NPUS_PER_NODE",
    )
    parser.add_argument(
        "--confirm-checkpoint-exclusive",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--require-resumable-checkpoint",
        action="store_true",
        help="require a strict committed checkpoint suitable for recovery",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="start without loading a checkpoint in a new run archive",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    try:
        manifest_path = create_injection(
            source_path=args.source.resolve(),
            hccl_evidence=args.hccl_evidence.resolve(),
            ping_evidence=args.hccl_ping_evidence.resolve(),
            output_dir=args.output_dir.resolve(),
            run_id=args.run_id,
            training_cwd=args.training_cwd,
            master_port=args.master_port,
            allow_topology_change=args.allow_topology_change,
            require_resumable_checkpoint=args.require_resumable_checkpoint,
            fresh_start=args.fresh,
        )
    except (InjectionError, OSError, UnicodeError) as error:
        print(f"STOP: training parameter injection failed: {error}", file=sys.stderr)
        return 1
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("topologyChange", {}).get("changed") is True:
        print(
            "WARNING: topology change was explicitly approved; verify that "
            "the checkpoint is compatible and no other job writes its save directory.",
            file=sys.stderr,
        )
    if manifest.get("launchMode") == "fresh":
        print(
            "FRESH START: a create-only worker archive will be prepared at "
            f"{manifest['runtime']['archiveRoot']}.",
            file=sys.stderr,
        )
    elif manifest.get("checkpointWrite", {}).get("enabled") is True:
        print(
            "CHECKPOINT WRITE: enabled at "
            f"{manifest['checkpointWrite']['saveDir']}.",
            file=sys.stderr,
        )
    print(f"PASS: worker training scripts created at {manifest_path.parent}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
