from __future__ import annotations

from kyc_streaming.config import PlatformConfig
from kyc_streaming.io import read_delta_batch, write_delta_batch
from kyc_streaming.session import build_spark_session
from kyc_streaming.transformations import build_customer_risk_snapshot, build_tenant_daily_risk_mart


def main() -> None:
    config = PlatformConfig()
    spark = build_spark_session("kyc-batch-gold-customer-risk")
    silver_df = read_delta_batch(spark, config.silver_path)
    write_delta_batch(build_customer_risk_snapshot(silver_df), path=f"{config.gold_path}/customer_risk_snapshot", partition_by=["tenant_id"])
    write_delta_batch(build_tenant_daily_risk_mart(silver_df), path=f"{config.gold_path}/tenant_daily_risk_mart", partition_by=["tenant_id", "event_date"])


if __name__ == "__main__":
    main()
