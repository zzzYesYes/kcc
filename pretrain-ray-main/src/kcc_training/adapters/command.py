"""A small shell-free command boundary used by CLI-based adapters."""

from __future__ import annotations

from dataclasses import dataclass
import subprocess
from typing import Sequence


@dataclass(frozen=True)
class CommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


class CommandError(RuntimeError):
    def __init__(self, result: CommandResult):
        detail = result.stderr.strip() or result.stdout.strip() or "no output"
        super().__init__(
            f"command failed with exit code {result.returncode}: "
            f"{result.argv[0]}: {detail}"
        )
        self.result = result


class SubprocessRunner:
    def run(
        self,
        argv: Sequence[str],
        *,
        timeout_seconds: float = 30,
        input_text: str | None = None,
        check: bool = True,
    ) -> CommandResult:
        command = tuple(argv)
        if not command or not all(isinstance(item, str) and item for item in command):
            raise ValueError("command must be a non-empty sequence of strings")
        try:
            completed = subprocess.run(
                command,
                input=input_text,
                text=True,
                capture_output=True,
                shell=False,
                check=False,
                timeout=timeout_seconds,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise RuntimeError(f"cannot execute {command[0]}: {error}") from error
        result = CommandResult(
            argv=command,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )
        if check and result.returncode != 0:
            raise CommandError(result)
        return result

