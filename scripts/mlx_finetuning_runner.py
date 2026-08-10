#!/usr/bin/env python3
"""Mac-host MLX-LM fine-tuning wrapper used by the Fine Tuning UI."""

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

import argparse
import subprocess
import sys
from pathlib import Path


REQUIRED_FILES = ("train.jsonl", "valid.jsonl", "test.jsonl")


def validate_dataset(path: Path) -> None:
    if str(path).startswith("~"):
        raise ValueError("dataset path must be absolute or project-relative; expand '~' before running")
    missing = [name for name in REQUIRED_FILES if not (path / name).is_file()]
    if missing:
        raise ValueError(f"dataset is missing: {', '.join(missing)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run MLX-LM fine tuning from the project directory.")
    parser.add_argument("--model", required=True)
    parser.add_argument("--train", action="store_true", help="run training mode (accepted for MLX-LM command compatibility)")
    parser.add_argument(
        "--data",
        required=True,
        help="absolute or project-relative directory containing train.jsonl, valid.jsonl, test.jsonl",
    )
    parser.add_argument("--adapter-path", required=True)
    parser.add_argument("--iters", type=int, default=600)
    args = parser.parse_args()
    if args.iters < 1:
        parser.error("--iters must be positive")
    try:
        validate_dataset(Path(args.data))
    except ValueError as exc:
        parser.error(str(exc))
    command = [
        "mlx_lm.lora", "--model", args.model, "--train", "--data", args.data,
        "--adapter-path", args.adapter_path, "--iters", str(args.iters),
    ]
    print("Starting MLX fine tuning:", " ".join(command), flush=True)
    return subprocess.call(command)


if __name__ == "__main__":
    sys.exit(main())
