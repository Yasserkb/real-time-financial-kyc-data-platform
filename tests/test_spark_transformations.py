from __future__ import annotations

import json
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from ingestion.producers.event_factory import build_customer_journey
from kyc_streaming.schemas import KYC_EVENT_SCHEMA
from kyc_streaming.transformations import build_customer_risk_snapshot, normalize_bronze_events


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("kyc-transformations-test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _stringify_map(values: dict[str, Any] | None) -> dict[str, str | None]:
    """Normalize sparse Python payloads to the Spark map<string,string> contract."""
    if not values:
        return {}

    normalized: dict[str, str | None] = {}
    for key, value in values.items():
        if value is None:
            normalized[key] = None
        elif isinstance(value, (dict, list)):
            normalized[key] = json.dumps(value, sort_keys=True)
        else:
            normalized[key] = str(value)
    return normalized


def _events_df(spark: SparkSession, events: list[dict[str, Any]]) -> DataFrame:
    """Create test DataFrames with the same schema used by parsed Kafka events.

    Relying on Spark's inferred schema for nested Python dictionaries is unstable:
    sparse payload keys can be dropped when the first row does not contain them.
    """
    rows = [
        {
            **event,
            "payload": _stringify_map(event.get("payload")),
            "metadata": _stringify_map(event.get("metadata")),
        }
        for event in events
    ]
    return spark.createDataFrame(rows, schema=KYC_EVENT_SCHEMA)


def test_normalize_bronze_events_keeps_valid_records(spark: SparkSession) -> None:
    events = build_customer_journey()
    normalized_df = normalize_bronze_events(_events_df(spark, events))
    rows = normalized_df.collect()

    assert len(rows) == len(events)
    assert {row.is_valid for row in rows} == {"true"}


def test_customer_risk_snapshot_flags_watchlist_match(spark: SparkSession) -> None:
    events = build_customer_journey()
    customer_id = events[0]["customer_id"]
    tenant_id = events[0]["tenant_id"]

    events.append(
        {
            **events[0],
            "event_id": "00000000-0000-0000-0000-000000000999",
            "event_type": "WATCHLIST_SCREENING_COMPLETED",
            "tenant_id": tenant_id,
            "customer_id": customer_id,
            "payload": {"screening_status": "CONFIRMED_MATCH"},
        }
    )

    silver_df = normalize_bronze_events(_events_df(spark, events))
    snapshot_df = build_customer_risk_snapshot(silver_df)

    row = snapshot_df.filter(
        (snapshot_df.tenant_id == tenant_id) & (snapshot_df.customer_id == customer_id)
    ).collect()[0]

    assert row.has_confirmed_watchlist_match == 1
    assert row.customer_risk_band == "CRITICAL"