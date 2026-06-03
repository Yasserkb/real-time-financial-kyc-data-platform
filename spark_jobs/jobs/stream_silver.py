from __future__ import annotations

from kyc_streaming.config import PlatformConfig
from kyc_streaming.io import read_delta_stream, write_delta_stream
from kyc_streaming.session import build_spark_session
from kyc_streaming.transformations import normalize_bronze_events


def main() -> None:
    config = PlatformConfig()
    spark = build_spark_session("kyc-stream-silver")
    silver_df = normalize_bronze_events(read_delta_stream(spark, config.bronze_path))
    query = write_delta_stream(silver_df, path=config.silver_path, checkpoint_path=f"{config.checkpoint_path}/silver", query_name="kyc_silver_events", partition_by=["tenant_id", "event_date"], trigger_processing_time=config.trigger_processing_time)
    query.awaitTermination()


if __name__ == "__main__":
    main()
