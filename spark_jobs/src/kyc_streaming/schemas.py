from __future__ import annotations

from pyspark.sql.types import IntegerType, MapType, StringType, StructField, StructType

KYC_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_version", IntegerType(), nullable=False),
        StructField("tenant_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("correlation_id", StringType(), nullable=True),
        StructField("occurred_at", StringType(), nullable=False),
        StructField("produced_at", StringType(), nullable=False),
        StructField("payload", MapType(StringType(), StringType()), nullable=True),
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),
    ]
)
