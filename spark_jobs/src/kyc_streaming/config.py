from __future__ import annotations

from dataclasses import dataclass
from os import getenv


@dataclass(frozen=True)
class PlatformConfig:
    kafka_bootstrap_servers: str = getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    kyc_events_topic: str = getenv("KYC_EVENTS_TOPIC", "kyc.events.v1")
    bronze_path: str = getenv("KYC_BRONZE_PATH", "/tmp/kyc-lakehouse/bronze/kyc_events")
    silver_path: str = getenv("KYC_SILVER_PATH", "/tmp/kyc-lakehouse/silver/kyc_events")
    quarantine_path: str = getenv("KYC_QUARANTINE_PATH", "/tmp/kyc-lakehouse/quarantine/kyc_events")
    gold_path: str = getenv("KYC_GOLD_PATH", "/tmp/kyc-lakehouse/gold")
    checkpoint_path: str = getenv("KYC_CHECKPOINT_PATH", "/tmp/kyc-lakehouse/checkpoints")
    trigger_processing_time: str = getenv("SPARK_TRIGGER_PROCESSING_TIME", "15 seconds")
    watermark_delay: str = getenv("KYC_WATERMARK_DELAY", "10 days")
