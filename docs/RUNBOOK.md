# Runbook

## Broker check

```bash
docker exec kyc-redpanda rpk cluster health
docker exec kyc-redpanda rpk topic list
```

## Recreate topics

```bash
make topics
```

## Validate producer contract

```bash
python -c "from ingestion.producers.event_factory import build_kyc_event; from ingestion.producers.contract_validator import validate_event; e=build_kyc_event(); validate_event(e); print(e)"
```

## dbt debug

```bash
cd dbt_kyc
DBT_PROFILES_DIR=. dbt debug --target local
DBT_PROFILES_DIR=. dbt build --target local --select stg_kyc__events
```

## SLO examples

| Metric | Target |
| --- | --- |
| Ingestion delay | < 2 minutes |
| Invalid Silver row ratio | < 1% |
| Daily reconciliation completion | before 06:00 UTC |
| dbt mart freshness | < 24 hours |
