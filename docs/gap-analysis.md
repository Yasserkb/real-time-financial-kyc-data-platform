# Gap analysis

| Requirement | Current state | Action |
|---|---|---|
| Versioned contracts | JSON Schema and Avro are checked in | Add compatibility tests for every future schema version |
| Immutable Bronze | Raw event and Kafka metadata are retained | Add object-lock/retention controls in a real production profile |
| Deduplication | Silver uses watermark plus tenant/event ID deduplication | Record duplicate-rate metrics and test restart behavior in containers |
| Quarantine | Invalid normalized rows are written separately with a reason | Add an operator-approved reprocess workflow |
| Late data | Configurable ten-day event-time watermark exists | Add fixed late/out-of-order fixtures to CI |
| Analytics | dbt staging, intermediate, marts and tests exist | Run full `dbt build`, not parse only, in CI |
| Quality | Spark validation, GE suite and dbt tests exist | Connect GE execution to the quarantine/metrics path |
| Orchestration | Airflow controls reconciliation and dbt | Add replay and maintenance DAGs with bounded parameters |
| Lineage | Documentation and OpenLineage configuration hooks exist | Capture a Marquez-backed local lineage demonstration |
| Observability | Spark/Redpanda expose native metrics | Add a committed dashboard and deterministic health report |
