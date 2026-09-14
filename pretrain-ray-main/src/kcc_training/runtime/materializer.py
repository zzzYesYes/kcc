from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from kcc_training.artifacts import ArtifactGateway, ArtifactRef, materialize
from .spec import MAX_SPEC_BYTES, RuntimeSpec


class MaterializerError(RuntimeError):
    pass


def load_artifact_plan(path: Path) -> tuple[RuntimeSpec, tuple[tuple[str, Path], ...]]:
    spec = RuntimeSpec.load(path)
    if path.stat().st_size > MAX_SPEC_BYTES:
        raise MaterializerError("runtime spec is too large")
    document = json.loads(path.read_text(encoding="utf-8"))
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise MaterializerError("artifact plan is missing")
    plan: list[tuple[str, Path]] = []
    for key in ("source", "model", "data"):
        item = artifacts.get(key)
        if not isinstance(item, Mapping):
            raise MaterializerError(f"artifact {key} has no materialization plan")
        uri = item.get("uri")
        target = item.get("target")
        if not isinstance(uri, str) or not isinstance(target, str):
            raise MaterializerError(f"artifact {key} plan is invalid")
        destination = Path(target)
        if not destination.is_absolute():
            raise MaterializerError(f"artifact {key} target must be absolute")
        plan.append((uri, destination))
    return spec, tuple(plan)


def execute(path: Path, gateway: ArtifactGateway) -> tuple[Path, ...]:
    _spec, plan = load_artifact_plan(path)
    outputs: list[Path] = []
    for uri, destination in plan:
        resolved = gateway.resolve(ArtifactRef.parse(uri))
        outputs.append(materialize(resolved, destination))
    return tuple(outputs)


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--gateway", default=os.environ.get("KCC_ARTIFACT_GATEWAY"))
    parser.add_argument("--token-file", type=Path, default=os.environ.get("KCC_ARTIFACT_TOKEN_FILE"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    if not args.gateway:
        raise SystemExit("artifact gateway is required")
    token = None
    if args.token_file is not None:
        token = args.token_file.read_text(encoding="utf-8").strip()
    outputs = execute(args.spec, ArtifactGateway(args.gateway, token=token))
    print(json.dumps({"status": "PASS", "paths": [str(path) for path in outputs]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

