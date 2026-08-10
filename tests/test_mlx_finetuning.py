import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from mlx_finetuning import build_mlx_lora_command, estimate_mlx_duration
from mlx_finetuning_runner import REQUIRED_FILES, validate_dataset


def test_build_mlx_command_quotes_paths_and_includes_training_controls() -> None:
    command = build_mlx_lora_command(
        "mlx-community/Model",
        "data/training-set",
        "models/adapters",
        iterations=42,
        rank=16,
    )
    assert "python scripts/mlx_finetuning_runner.py --model mlx-community/Model" in command
    assert "--train --data data/training-set" in command
    assert "--adapter-path models/adapters" in command
    assert "--iters 42" in command
    assert "--lora-rank" not in command


@pytest.mark.parametrize("field", ["model", "data", "output"])
def test_build_mlx_command_requires_paths(field: str) -> None:
    values = {"model": "model", "data": "data/training-set", "output": "output"}
    values[field] = ""
    with pytest.raises(ValueError, match="required"):
        build_mlx_lora_command(**values)


def test_build_mlx_command_rejects_unsafe_iteration_values() -> None:
    with pytest.raises(ValueError, match="iterations"):
        build_mlx_lora_command("model", "data/training-set", "output", iterations=0)


def test_build_mlx_command_rejects_single_jsonl_input() -> None:
    with pytest.raises(ValueError, match="jsonl"):
        build_mlx_lora_command("model", "training.jsonl", "output")


def test_build_mlx_command_accepts_absolute_training_directory() -> None:
    command = build_mlx_lora_command("model", "/Users/example/Training Data", "models/output")

    assert "--data '/Users/example/Training Data'" in command


def test_build_mlx_command_requires_relative_output_path() -> None:
    with pytest.raises(ValueError, match="output path must be relative"):
        build_mlx_lora_command("model", "data/training", "/tmp/models/output")


def test_build_mlx_command_requires_expanded_home_directory() -> None:
    with pytest.raises(ValueError, match="expand '~'"):
        build_mlx_lora_command("model", "~/training", "models/output")


def test_validate_dataset_accepts_absolute_directory(tmp_path: Path) -> None:
    for name in REQUIRED_FILES:
        (tmp_path / name).write_text("{}\n")

    validate_dataset(tmp_path)


def test_validate_dataset_reports_missing_files(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="train.jsonl, valid.jsonl, test.jsonl"):
        validate_dataset(tmp_path)


def test_validate_dataset_requires_expanded_home_directory() -> None:
    with pytest.raises(ValueError, match="expand '~'"):
        validate_dataset(Path("~/training"))


def test_estimate_mlx_duration_scales_with_iterations_and_model_size() -> None:
    small = estimate_mlx_duration("3B-model", 600, 8)
    large = estimate_mlx_duration("8B-model", 600, 8)
    assert small["seconds"] > 0
    assert large["seconds"] > small["seconds"]
    assert "Planning estimate" in str(small["basis"])
