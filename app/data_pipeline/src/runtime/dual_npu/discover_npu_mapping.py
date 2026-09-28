#!/usr/bin/env python3
"""Discover logical device IDs for physical Ascend chips 14 and 15."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path


def command(*args: str) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def discover() -> dict[str, dict[str, int | str]]:
    devices: dict[str, dict[str, int | str]] = {}
    for line in command("npu-smi", "info", "-m").splitlines():
        fields = line.split()
        if len(fields) != 5 or fields[-1] != "Ascend910":
            continue
        npu_id, chip_id, logical_id, physical_id, _ = fields
        if physical_id not in {"14", "15"}:
            continue
        device_path = Path(f"/dev/davinci{logical_id}")
        if not device_path.exists():
            raise RuntimeError(f"missing mapped device {device_path} for physical {physical_id}")
        devices[physical_id] = {
            "physical_id": int(physical_id),
            "logical_id": int(logical_id),
            "npu_id": int(npu_id),
            "chip_id": int(chip_id),
            "device_path": str(device_path),
            "device_realpath": os.path.realpath(device_path),
        }
    missing = {"14", "15"} - devices.keys()
    if missing:
        raise RuntimeError(f"physical NPU mapping missing for {sorted(missing)}")
    return devices


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("/tmp/mineru-dual/npu-mapping.json"))
    args = parser.parse_args()
    mapping = {
        "devices": discover(),
        "dev_listing": command("ls", "-l", "/dev/davinci14", "/dev/davinci15"),
        "npu_smi_mapping": command("npu-smi", "info", "-m"),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(mapping["devices"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
