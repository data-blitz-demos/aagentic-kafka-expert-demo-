#!/usr/bin/env python3
"""Generate realistic, entirely synthetic personal health record payloads.

The producer UI uses this module for its Personal Health Record profile.  The
values are intentionally fictional (including ``example.invalid`` email
addresses and 555 phone numbers) and are suitable for demos and tests only.
They follow the shape documented by :file:`schemas/ph-schema.json`.
"""

from __future__ import annotations

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

from datetime import date, datetime, timedelta, timezone
import random
import uuid
from typing import Any


_PATIENTS = (
    ("Alex", "Morgan", "1986-04-18", "555-010-1842"),
    ("Jordan", "Rivera", "1979-11-02", "555-010-2275"),
    ("Taylor", "Nguyen", "1992-08-27", "555-010-3419"),
    ("Casey", "Bennett", "1968-01-15", "555-010-4066"),
)


def _timestamp(value: datetime) -> str:
    """Return a UTC timestamp in the format expected by the JSON schema."""
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _date(value: date) -> str:
    """Return an ISO calendar date."""
    return value.isoformat()


def generate_personal_health_record(
    *, now: datetime | None = None, rng: random.Random | None = None
) -> dict[str, Any]:
    """Build one plausible synthetic longitudinal health record.

    ``now`` and ``rng`` are injectable so tests can make generation
    deterministic without changing the producer's normal randomized output.
    """
    clock = now or datetime.now(timezone.utc)
    randomizer = rng or random
    given_name, family_name, birth_date, phone = randomizer.choice(_PATIENTS)
    patient_id = str(uuid.uuid4())
    recorded_at = _timestamp(clock)
    encounter_start = clock - timedelta(days=randomizer.randint(7, 90))
    encounter_end = encounter_start + timedelta(minutes=randomizer.randint(18, 48))
    condition_code = {
        "system": "http://snomed.info/sct",
        "code": "38341003",
        "display": "Essential hypertension",
    }

    return {
        "recordId": patient_id,
        "patient": {
            "name": f"{given_name} {family_name}",
            "givenName": given_name,
            "familyName": family_name,
            "birthDate": birth_date,
            "sexAtBirth": randomizer.choice(["female", "male"]),
            "genderIdentity": randomizer.choice(["woman", "man", "non-binary"]),
            "pronouns": randomizer.choice(["she/her", "he/him", "they/them"]),
            "phone": phone,
            "email": f"{given_name.lower()}.{family_name.lower()}@example.invalid",
            "address": f"{randomizer.randint(100, 8999)} Demo Avenue, Springfield, IL 62704",
            "emergencyContact": {
                "name": "Jamie Morgan",
                "relationship": "partner",
                "phone": "555-010-9911",
                "email": "jamie.morgan@example.invalid",
            },
        },
        "lastUpdated": recorded_at,
        "conditions": [
            {
                "code": condition_code,
                "status": "active",
                "recordedAt": recorded_at,
                "onset": "2021-06-12",
                "notes": "Controlled with medication and lifestyle changes.",
                "provider": "Dr. Avery Patel",
            }
        ],
        "allergies": [
            {
                "substance": "Penicillin",
                "reaction": "rash",
                "severity": "moderate",
                "status": "active",
                "recordedAt": recorded_at,
            }
        ],
        "medications": [
            {
                "name": "Lisinopril",
                "rxNormCode": "314077",
                "dose": "10 mg",
                "route": "oral",
                "frequency": "once daily",
                "status": "active",
                "period": {"start": "2021-06-12T00:00:00Z"},
                "prescriber": "Dr. Avery Patel",
                "notes": "Take in the morning.",
            }
        ],
        "immunizations": [
            {
                "vaccine": "Influenza, injectable, quadrivalent",
                "cvxCode": "150",
                "administeredAt": _timestamp(clock - timedelta(days=42)),
                "doseNumber": 1,
                "lotNumber": "SYNTH-2025-01",
                "administeredBy": "Demo Family Clinic",
            }
        ],
        "observations": [
            {
                "name": "Blood pressure",
                "loincCode": "85354-9",
                "value": "128/82",
                "unit": "mmHg",
                "referenceRange": "below 130/80",
                "observedAt": recorded_at,
                "status": "final",
                "notes": "Home reading reported by patient.",
            },
            {
                "name": "Body weight",
                "loincCode": "29463-7",
                "value": round(randomizer.uniform(62.0, 96.0), 1),
                "unit": "kg",
                "referenceRange": "not established",
                "observedAt": recorded_at,
                "status": "final",
            },
        ],
        "encounters": [
            {
                "type": "ambulatory",
                "provider": "Dr. Avery Patel",
                "organization": "Demo Family Clinic",
                "startedAt": _timestamp(encounter_start),
                "endedAt": _timestamp(encounter_end),
                "reason": "Routine hypertension follow-up",
                "diagnoses": [condition_code],
                "notes": "Continue current therapy; follow up in six months.",
            }
        ],
        "procedures": [
            {
                "code": {
                    "system": "http://snomed.info/sct",
                    "code": "710824005",
                    "display": "Measurement of blood pressure",
                },
                "performedAt": recorded_at,
                "provider": "Demo Family Clinic",
                "outcome": "completed",
            }
        ],
        "carePlans": [
            {
                "title": "Hypertension self-management",
                "status": "active",
                "goals": ["Maintain home blood pressure below 130/80 mmHg"],
                "activities": ["Walk for 30 minutes five days per week", "Record blood pressure weekly"],
                "startDate": "2025-01-10",
                "reviewDate": _date((clock + timedelta(days=120)).date()),
            }
        ],
        "insurance": [
            {
                "payer": "Demo Health Plan",
                "memberId": f"SYN-{randomizer.randint(10000000, 99999999)}",
                "planName": "Everyday Choice PPO",
                "groupNumber": "DEMO-042",
                "effectiveDate": "2025-01-01",
            }
        ],
        "consents": [
            {
                "purpose": "care coordination",
                "status": "active",
                "scope": "primary care and referred specialists",
                "recordedAt": recorded_at,
                "expiresAt": _timestamp(clock + timedelta(days=365)),
            }
        ],
        "provenance": [
            {
                "source": "data-blitz-synthetic-phr-generator",
                "sourceRecordId": patient_id,
                "recordedAt": recorded_at,
                "recordedBy": "producer-ui",
            }
        ],
    }

