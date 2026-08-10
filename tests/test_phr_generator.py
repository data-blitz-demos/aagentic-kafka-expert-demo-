"""Tests for realistic synthetic Personal Health Record generation."""

from datetime import datetime, timezone
import json
from pathlib import Path
import random
import re
import sys

from jsonschema import Draft202012Validator, FormatChecker

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from phr_generator import generate_personal_health_record


SCHEMA = Path(__file__).resolve().parents[1] / "schemas" / "ph-schema.json"
PRODUCER_UI = Path(__file__).resolve().parents[1] / "scripts" / "producer_ui.py"
PRODUCER_CLI = Path(__file__).resolve().parents[1] / "scripts" / "producer.py"


def test_generated_record_has_all_required_sections_and_safe_synthetic_identifiers() -> None:
    record = generate_personal_health_record(
        now=datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc), rng=random.Random(7)
    )

    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert set(schema["required"]).issubset(record)
    assert re.fullmatch(r"[0-9a-f-]{36}", record["recordId"])
    assert record["patient"]["email"].endswith("@example.invalid")
    assert record["patient"]["phone"].startswith("555-")
    assert record["provenance"][0]["source"] == "data-blitz-synthetic-phr-generator"
    assert record["lastUpdated"] == "2026-08-03T12:00:00Z"


def test_generated_record_fully_validates_against_ph_schema() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    record = generate_personal_health_record(rng=random.Random(19))
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(record),
        key=lambda error: list(error.path),
    )
    assert errors == []


def test_generated_record_has_temporally_consistent_clinical_events() -> None:
    record = generate_personal_health_record(
        now=datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc), rng=random.Random(11)
    )

    encounter = record["encounters"][0]
    assert encounter["startedAt"] < encounter["endedAt"] <= record["lastUpdated"]
    assert record["observations"]
    assert all(item["status"] == "final" for item in record["observations"])
    assert record["conditions"][0]["code"]["code"] in {"38341003"}
    assert record["medications"][0]["status"] == "active"
    assert record["consents"][0]["expiresAt"] > record["consents"][0]["recordedAt"]


def test_generator_does_not_emit_unapproved_top_level_fields() -> None:
    record = generate_personal_health_record(rng=random.Random(3))
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert set(record).issubset(schema["properties"])


def test_many_random_records_validate_and_have_unique_record_ids() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    records = [generate_personal_health_record(rng=random.Random(seed)) for seed in range(50)]
    assert len({record["recordId"] for record in records}) == len(records)
    for record in records:
        assert list(validator.iter_errors(record)) == []


def test_producer_ui_exposes_phr_profile_and_json_topic_contract() -> None:
    text = PRODUCER_UI.read_text(encoding="utf-8")
    assert 'PHR_TOPIC = os.getenv("PHR_TOPIC", "personal-health-records")' in text
    assert 'option value="phr"' in text
    assert '@app.post("/api/profile")' in text
    assert 'value=json.dumps(message).encode("utf-8")' in text
    assert '"profile": self.profile' in text


def test_cli_producer_has_phr_profile_switch() -> None:
    text = PRODUCER_CLI.read_text(encoding="utf-8")
    assert 'PRODUCER_PROFILE' in text
    assert 'PHR_TOPIC' in text
    assert 'generate_personal_health_record() if profile == "phr"' in text
    assert 'json.dumps(record).encode("utf-8") if profile == "phr"' in text
