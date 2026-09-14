"""Engineering CLI for validation and non-mutating environment diagnosis."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Sequence

from . import __version__
from .contracts import ContractError, load_contract
from .doctor import as_jsonable, run_checks, successful


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="kcc-training")
    result.add_argument("--version", action="version", version=__version__)
    commands = result.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="validate a public API document")
    validate.add_argument("path", type=Path)
    doctor = commands.add_parser("doctor", help="run read-only local dependency checks")
    doctor.add_argument("--json", action="store_true")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "validate":
        try:
            value = load_contract(args.path)
        except ContractError as error:
            print(f"invalid: {error}", file=sys.stderr)
            return 2
        print(f"valid: {type(value).__name__} {value.name}")
        return 0
    checks = run_checks(PROJECT_ROOT)
    if args.json:
        print(json.dumps(as_jsonable(checks), ensure_ascii=False, indent=2))
    else:
        for check in checks:
            level = "PASS" if check.ok else ("FAIL" if check.required else "WARN")
            print(f"{level:4} {check.name}: {check.detail}")
    return 0 if successful(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())

