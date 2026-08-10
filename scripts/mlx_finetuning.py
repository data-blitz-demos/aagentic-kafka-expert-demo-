"""Helpers for preparing MLX-LM fine-tuning runs on the Mac host."""

from __future__ import annotations

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

import importlib.util
import platform
import shlex
from pathlib import PurePath


def mlx_runtime_status() -> dict[str, object]:
    """Describe whether the current Python environment can import MLX-LM."""
    return {
        "platform": f"{platform.system()} {platform.machine()}".strip(),
        "mlx_available": importlib.util.find_spec("mlx") is not None,
        "mlx_lm_available": importlib.util.find_spec("mlx_lm") is not None,
        "execution": "mac_host",
    }


def build_mlx_lora_command(
    model: str,
    data: str,
    output: str,
    iterations: int = 600,
    rank: int = 8,
) -> str:
    """Build a shell-safe MLX-LM LoRA command; execution remains host-side."""
    model, data, output = (str(value).strip() for value in (model, data, output))
    if not model or not data or not output:
        raise ValueError("model, data, and output are required")
    if data.startswith("~"):
        raise ValueError("data path must be absolute or project-relative; expand '~' before submitting")
    if PurePath(output).is_absolute() or output.startswith("~"):
        raise ValueError("output path must be relative")
    if PurePath(data).suffix.lower() == ".jsonl":
        raise ValueError("data must be a dataset directory containing train.jsonl, valid.jsonl, and test.jsonl")
    if iterations < 1 or iterations > 100000:
        raise ValueError("iterations must be between 1 and 100000")
    if rank < 1 or rank > 256:
        raise ValueError("rank must be between 1 and 256")
    return " ".join(
        [
            "python scripts/mlx_finetuning_runner.py",
            "--model", shlex.quote(model),
            "--train --data", shlex.quote(data),
            "--adapter-path", shlex.quote(output),
            "--iters", str(iterations),
        ]
    )


def build_mlx_lora_args(model: str, data: str, output: str, iterations: int = 600, rank: int = 8) -> list[str]:
    """Build argv for subprocess execution without invoking a shell."""
    command = build_mlx_lora_command(model, data, output, iterations, rank)
    return shlex.split(command)


def estimate_mlx_duration(model: str, iterations: int, rank: int) -> dict[str, object]:
    """Return a deliberately conservative, hardware-independent planning estimate."""
    name = str(model).lower()
    model_factor = 2.2 if any(token in name for token in ("7b", "8b", "13b")) else 1.0
    seconds = max(1, int(iterations)) * 0.45 * model_factor * max(1, int(rank)) / 8
    minutes = seconds / 60
    return {
        "seconds": round(seconds, 1),
        "minutes": round(minutes, 1),
        "label": f"about {minutes:.1f} minutes",
        "basis": "Planning estimate; actual time varies with Mac GPU, sequence length, and dataset size.",
    }
