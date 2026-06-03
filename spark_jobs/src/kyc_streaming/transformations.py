from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StructType

from kyc_streaming.schemas import KYC_EVENT_SCHEMA

ALLOWED_EVENT_TYPES = [
    "CUSTOMER_ONBOARDED",
    "DOCUMENT_UPLOADED",
    "DOCUMENT_CONTROL_COMPLETED",
    "IDENTITY_VERIFICATION_COMPLETED",
    "WATCHLIST_SCREENING_COMPLETED",
    "RISK_SCORE_UPDATED",
    "CASE_DECISIONED",
]

def coalesce_existing_or_add(df: DataFrame, column_name: str, default_value: Column) -> DataFrame:
    """Coalesce an existing column or add it when the upstream source has not produced it yet."""
    if column_name in df.columns:
        return df.withColumn(column_name, F.coalesce(F.col(column_name), default_value))
    return df.withColumn(column_name, default_value)

def parse_kafka_json_events(kafka_df: DataFrame) -> DataFrame:
    """Convert Kafka binary key/value records into typed KYC event rows."""
    return (
        kafka_df.select(
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("raw_event"),
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("parsed", F.from_json("raw_event", KYC_EVENT_SCHEMA))
        .select("message_key", "raw_event", "topic", "partition", "offset", "kafka_timestamp", "parsed.*")
        .withColumn("ingested_at", F.current_timestamp())
    )

def add_bronze_metadata(events_df: DataFrame) -> DataFrame:
    """Add deterministic audit metadata to raw events."""
    return (
        events_df.withColumn("event_date", F.to_date(F.to_timestamp("occurred_at")).cast("string"))
        .withColumn("bronze_loaded_at", F.current_timestamp())
        .withColumn("source_system", F.coalesce(F.col("metadata")["producer"], F.lit("unknown")))
        .withColumn("record_hash", F.sha2(F.concat_ws("||", "event_id", "tenant_id", "customer_id", "raw_event"), 256))
    )

def normalize_bronze_events(bronze_df: DataFrame) -> DataFrame:
    """Normalize raw events into Silver rows with sparse event-specific fields.

    Payloads are sparse by design: each event type carries only the fields that
    belong to that business event. This function safely extracts optional fields
    whether payload is represented as MapType, StructType, or JSON string.
    """
    bronze_with_metadata = coalesce_existing_or_add(
        bronze_df,
        "ingested_at",
        F.current_timestamp(),
    )

    normalized = (
        bronze_with_metadata
        .withColumn("occurred_at_ts", F.to_timestamp("occurred_at"))
        .withColumn("produced_at_ts", F.to_timestamp("produced_at"))
        .withColumn("document_type", payload_string_field(bronze_with_metadata, "document_type"))
        .withColumn("control_status", payload_string_field(bronze_with_metadata, "control_status"))
        .withColumn("verification_status", payload_string_field(bronze_with_metadata, "verification_status"))
        .withColumn("screening_status", payload_string_field(bronze_with_metadata, "screening_status"))
        .withColumn("risk_score", payload_string_field(bronze_with_metadata, "risk_score").cast("int"))
        .withColumn("risk_band", payload_string_field(bronze_with_metadata, "risk_band"))
        .withColumn("decision", payload_string_field(bronze_with_metadata, "decision"))
        .withColumn("event_date", F.to_date("occurred_at_ts").cast("string"))
    )

    return add_quality_columns(normalized).select(
        "event_id",
        "event_type",
        "event_version",
        "tenant_id",
        "customer_id",
        "correlation_id",
        F.col("occurred_at_ts").alias("occurred_at"),
        F.col("produced_at_ts").alias("produced_at"),
        "ingested_at",
        "document_type",
        "control_status",
        "verification_status",
        "screening_status",
        "risk_score",
        "risk_band",
        "decision",
        "is_valid",
        "quality_error",
        "event_date",
    )

def add_quality_columns(df: DataFrame) -> DataFrame:
    invalid_reason = (
        F.when(F.col("event_id").isNull(), F.lit("missing_event_id"))
        .when(F.col("tenant_id").isNull(), F.lit("missing_tenant_id"))
        .when(F.col("customer_id").isNull(), F.lit("missing_customer_id"))
        .when(~F.col("event_type").isin(ALLOWED_EVENT_TYPES), F.lit("invalid_event_type"))
        .when(F.col("occurred_at_ts").isNull(), F.lit("invalid_occurred_at"))
        .when((F.col("risk_score") < 0) | (F.col("risk_score") > 100), F.lit("risk_score_out_of_range"))
    )
    return df.withColumn("quality_error", invalid_reason).withColumn(
        "is_valid", F.when(F.col("quality_error").isNull(), F.lit("true")).otherwise(F.lit("false"))
    )

def build_customer_risk_snapshot(silver_df: DataFrame) -> DataFrame:
    """Build customer-level Gold risk snapshot."""
    return (
        silver_df.filter(F.col("is_valid") == "true")
        .groupBy("tenant_id", "customer_id")
        .agg(
            F.max("occurred_at").alias("last_event_at"),
            F.count("event_id").alias("event_count"),
            F.max(F.when(F.col("event_type") == "CUSTOMER_ONBOARDED", F.col("occurred_at"))).alias("onboarded_at"),
            F.max("risk_score").alias("max_risk_score"),
            F.max(F.when(F.col("screening_status") == "CONFIRMED_MATCH", F.lit(1)).otherwise(F.lit(0))).alias("has_confirmed_watchlist_match"),
            F.max(F.when(F.col("control_status") == "REJECTED", F.lit(1)).otherwise(F.lit(0))).alias("has_rejected_document"),
            F.max(F.when(F.col("verification_status") == "FAILED", F.lit(1)).otherwise(F.lit(0))).alias("has_failed_identity_verification"),
            F.max("decision").alias("latest_decision"),
        )
        .withColumn(
            "customer_risk_band",
            F.when(F.col("has_confirmed_watchlist_match") == 1, F.lit("CRITICAL"))
            .when(F.col("max_risk_score") >= 75, F.lit("HIGH"))
            .when(F.col("max_risk_score") >= 40, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW")),
        )
        .withColumn("snapshot_at", F.current_timestamp())
    )

def build_tenant_daily_risk_mart(silver_df: DataFrame) -> DataFrame:
    """Build daily tenant-level operational mart."""
    return (
        silver_df.filter(F.col("is_valid") == "true")
        .groupBy("tenant_id", "event_date")
        .agg(
            F.countDistinct("customer_id").alias("active_customers"),
            F.count("event_id").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "CUSTOMER_ONBOARDED", 1).otherwise(0)).alias("onboarding_count"),
            F.sum(F.when(F.col("control_status") == "REJECTED", 1).otherwise(0)).alias("rejected_documents"),
            F.sum(F.when(F.col("screening_status") == "POTENTIAL_MATCH", 1).otherwise(0)).alias("potential_watchlist_matches"),
            F.sum(F.when(F.col("screening_status") == "CONFIRMED_MATCH", 1).otherwise(0)).alias("confirmed_watchlist_matches"),
            F.avg("risk_score").alias("avg_risk_score"),
            F.max("risk_score").alias("max_risk_score"),
        )
        .withColumn("mart_loaded_at", F.current_timestamp())
    )

def payload_string_field(df: DataFrame, field_name: str) -> Column:
    """Read an optional payload field from MapType, StructType, or JSON string payloads."""
    payload_type = df.schema["payload"].dataType

    if isinstance(payload_type, MapType):
        return F.col("payload").getItem(field_name).cast("string")

    if isinstance(payload_type, StructType):
        if field_name in payload_type.fieldNames():
            return F.col("payload").getField(field_name).cast("string")
        return F.lit(None).cast("string")

    return F.get_json_object(F.col("payload").cast("string"), f"$.{field_name}")