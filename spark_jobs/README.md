# Spark jobs

Reusable Spark transformation library plus executable streaming/batch jobs.

- `stream_bronze.py`: Kafka -> Bronze Delta-ready lake path.
- `stream_silver.py`: Bronze -> validated Silver events.
- `batch_gold_customer_risk.py`: Silver -> customer and tenant risk marts.
- `batch_reconciliation.py`: quality gate and reconciliation.
