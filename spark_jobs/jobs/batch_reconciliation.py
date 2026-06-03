from __future__ import annotations

from pyspark.sql import functions as F

from kyc_streaming.config import PlatformConfig
from kyc_streaming.io import read_delta_batch
from kyc_streaming.quality import assert_quality_gate
from kyc_streaming.session import build_spark_session


def main() -> None:
    config = PlatformConfig()
    spark = build_spark_session("kyc-batch-reconciliation")
    silver_df = read_delta_batch(spark, config.silver_path)
    assert_quality_gate(silver_df)
    silver_df.groupBy("tenant_id", "event_date").agg(F.count("*").alias("silver_rows"), F.countDistinct("event_id").alias("distinct_events"), F.sum(F.when(F.col("is_valid") == "false", 1).otherwise(0)).alias("invalid_rows")).orderBy("tenant_id", "event_date").show(100, truncate=False)


if __name__ == "__main__":
    main()
