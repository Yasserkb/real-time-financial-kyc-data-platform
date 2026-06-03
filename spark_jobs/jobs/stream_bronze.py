from __future__ import annotations

from kyc_streaming.config import PlatformConfig
from kyc_streaming.io import read_kafka_stream, write_delta_stream
from kyc_streaming.session import build_spark_session
from kyc_streaming.transformations import add_bronze_metadata, parse_kafka_json_events


def main() -> None:
    config = PlatformConfig()
    spark = build_spark_session("kyc-stream-bronze")
    bronze_df = add_bronze_metadata(parse_kafka_json_events(read_kafka_stream(spark, config)))
    query = write_delta_stream(bronze_df, path=config.bronze_path, checkpoint_path=f"{config.checkpoint_path}/bronze", query_name="kyc_bronze_events", partition_by=["tenant_id", "event_date"], trigger_processing_time=config.trigger_processing_time)
    query.awaitTermination()


if __name__ == "__main__":
    main()
