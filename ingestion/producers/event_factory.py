from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from uuid import uuid4

TENANTS = ["axa-fr", "ccf-fr", "amundi-fr", "demo-ma"]
COUNTRIES = ["FR", "MA", "BE", "DE", "ES"]
DOCUMENT_TYPES = ["PASSPORT", "NATIONAL_ID", "PROOF_OF_ADDRESS", "RESIDENCE_PERMIT"]
EVENT_TYPES = [
    "CUSTOMER_ONBOARDED",
    "DOCUMENT_UPLOADED",
    "DOCUMENT_CONTROL_COMPLETED",
    "IDENTITY_VERIFICATION_COMPLETED",
    "WATCHLIST_SCREENING_COMPLETED",
    "RISK_SCORE_UPDATED",
    "CASE_DECISIONED",
]


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _customer_id() -> str:
    return f"cus_{uuid4().hex[:16]}"


def _payload(event_type: str) -> dict:
    if event_type == "CUSTOMER_ONBOARDED":
        return {"customer_type": random.choice(["INDIVIDUAL", "PROFESSIONAL"]), "country_of_residence": random.choice(COUNTRIES), "onboarding_channel": random.choice(["WEB", "MOBILE"])}
    if event_type == "DOCUMENT_UPLOADED":
        return {"document_id": f"doc_{uuid4().hex[:14]}", "document_type": random.choice(DOCUMENT_TYPES), "file_count": random.randint(1, 3), "source_system": random.choice(["KYC_CORE", "ID360", "MANUAL_UPLOAD"])}
    if event_type == "DOCUMENT_CONTROL_COMPLETED":
        status = random.choices(["APPROVED", "REJECTED", "INCOMPLETE"], weights=[70, 15, 15])[0]
        return {"document_id": f"doc_{uuid4().hex[:14]}", "document_type": random.choice(DOCUMENT_TYPES), "control_status": status, "rejection_reason": None if status == "APPROVED" else random.choice(["EXPIRED", "BLURRY", "MISMATCH"])}
    if event_type == "IDENTITY_VERIFICATION_COMPLETED":
        return {"verification_status": random.choices(["VERIFIED", "FAILED", "REVIEW_REQUIRED"], weights=[78, 8, 14])[0], "provider": random.choice(["ID360", "MANUAL", "BANK_ID"]), "confidence_score": round(random.uniform(0.35, 0.99), 4)}
    if event_type == "WATCHLIST_SCREENING_COMPLETED":
        return {"screening_status": random.choices(["CLEAR", "POTENTIAL_MATCH", "CONFIRMED_MATCH"], weights=[90, 9, 1])[0], "matched_lists": random.sample(["PEP", "SANCTIONS", "ADVERSE_MEDIA"], k=random.randint(0, 2))}
    if event_type == "RISK_SCORE_UPDATED":
        score = random.randint(0, 100)
        return {"risk_score": score, "risk_band": "HIGH" if score >= 75 else "MEDIUM" if score >= 40 else "LOW", "reason_codes": random.sample(["DOC_REJECTED", "PEP_MATCH", "HIGH_RISK_COUNTRY", "LOW_CONFIDENCE_ID"], k=random.randint(0, 3))}
    return {"decision": random.choices(["ACCEPTED", "REJECTED", "ESCALATED"], weights=[82, 8, 10])[0], "decision_by": random.choice(["SYSTEM", "ANALYST", "SUPERVISOR"]), "sla_minutes": random.randint(5, 2880)}


def build_kyc_event(
    customer_id: str | None = None,
    event_type: str | None = None,
    tenant_id: str | None = None,
) -> dict:
    now = datetime.now(UTC)
    occurred_at = now - timedelta(seconds=random.randint(0, 60 * 60 * 24 * 10))
    selected_event_type = event_type or random.choice(EVENT_TYPES)
    selected_customer_id = customer_id or _customer_id()
    selected_tenant_id = tenant_id or random.choice(TENANTS)

    return {"event_id": str(uuid4()), "event_type": selected_event_type, "event_version": 1, "tenant_id": selected_tenant_id, "customer_id": selected_customer_id, "correlation_id": f"corr_{uuid4().hex[:16]}", "occurred_at": _iso(occurred_at), "produced_at": _iso(now), "payload": _payload(selected_event_type), "metadata": {"producer": "synthetic-kyc-event-producer", "environment": "local", "schema": "kyc_lifecycle_event.v1", "trace_id": uuid4().hex}}


def build_customer_journey(events_per_customer: int = 7) -> list[dict]:
    customer_id = _customer_id()
    tenant_id = random.choice(TENANTS)
    return [build_kyc_event(customer_id, event_type, tenant_id) for event_type in EVENT_TYPES[:events_per_customer]]