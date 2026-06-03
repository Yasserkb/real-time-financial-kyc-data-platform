# Architecture decisions

## ADR-001: Local-first, production-shaped

The project runs locally with Docker Compose, but every component has a clear production mapping. This makes the repo easy to inspect while still showing cloud architecture maturity.

## ADR-002: Preserve invalid data

Invalid rows are not silently dropped. Silver attaches `is_valid` and `quality_error`. In regulated data domains, auditability matters more than pretending bad data never existed.

## ADR-003: Spark owns lakehouse normalization, dbt owns analytical semantics

Spark handles streaming, typing, validation, and scalable aggregations. dbt handles transparent SQL modeling, tests, documentation, and BI-facing marts.

## ADR-004: Tenant isolation is non-negotiable

Every table keeps `tenant_id`, and partitioning includes tenant/date where useful. This is a core design choice for a multi-tenant banking/KYC domain.
