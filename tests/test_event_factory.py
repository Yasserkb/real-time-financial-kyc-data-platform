from __future__ import annotations

from ingestion.producers.contract_validator import validate_event
from ingestion.producers.event_factory import EVENT_TYPES, build_customer_journey, build_kyc_event


def test_generated_event_respects_contract() -> None:
    event = build_kyc_event(event_type="RISK_SCORE_UPDATED")
    validate_event(event)
    assert event["event_type"] == "RISK_SCORE_UPDATED"
    assert 0 <= event["payload"]["risk_score"] <= 100


def test_customer_journey_uses_same_customer_id() -> None:
    journey = build_customer_journey()
    customer_ids = {event["customer_id"] for event in journey}
    assert len(customer_ids) == 1
    assert [event["event_type"] for event in journey] == EVENT_TYPES


def test_contract_rejects_missing_required_field() -> None:
    event = build_kyc_event()
    event.pop("tenant_id")
    try:
        validate_event(event)
    except ValueError as exc:
        assert "tenant_id" in str(exc)
    else:
        raise AssertionError("contract validation should have failed")
