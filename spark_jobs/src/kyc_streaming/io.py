from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from kyc_streaming.config import PlatformConfig


def read_kafka_stream(spark: SparkSession, config: PlatformConfig) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kyc_events_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def read_delta_stream(spark: SparkSession, path: str) -> DataFrame:
    return spark.readStream.format("delta").load(path)


def read_delta_batch(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def write_delta_stream(df: DataFrame, path: str, checkpoint_path: str, query_name: str, partition_by: list[str], trigger_processing_time: str) -> StreamingQuery:
    return (
        df.writeStream.format("delta")
        .queryName(query_name)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .partitionBy(*partition_by)
        .trigger(processingTime=trigger_processing_time)
        .start(path)
    )


def write_delta_batch(df: DataFrame, path: str, partition_by: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
