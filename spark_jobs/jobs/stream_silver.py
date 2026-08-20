from __future__ import annotations

from kyc_streaming.config import PlatformConfig
from kyc_streaming.io import read_delta_stream, write_delta_stream
from kyc_streaming.session import build_spark_session
from kyc_streaming.transformations import normalize_bronze_events


def main() -> None:
    config = PlatformConfig()
    spark = build_spark_session("kyc-stream-silver")
    normalized = normalize_bronze_events(read_delta_stream(spark, config.bronze_path))
    # Deduplicate only accepted records. Invalid rows may have null identifiers;
    # collapsing those would destroy evidence needed for diagnosis/reprocessing.
    valid = (
        normalized.filter("is_valid = 'true'")
        .withWatermark("occurred_at", config.watermark_delay)
        .dropDuplicates(["tenant_id", "event_id"])
    )
    quarantine = normalized.filter("is_valid = 'false'")
    write_delta_stream(valid, path=config.silver_path, checkpoint_path=f"{config.checkpoint_path}/silver", query_name="kyc_silver_events", partition_by=["tenant_id", "event_date"], trigger_processing_time=config.trigger_processing_time)
    write_delta_stream(quarantine, path=config.quarantine_path, checkpoint_path=f"{config.checkpoint_path}/quarantine", query_name="kyc_quarantine_events", partition_by=["event_date"], trigger_processing_time=config.trigger_processing_time)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
