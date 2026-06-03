# Data quality strategy

Quality is layered:

1. JSON Schema validation before publishing events.
2. Spark Silver validation using `is_valid` and `quality_error`.
3. Reconciliation failure when invalid row ratio exceeds the threshold.
4. dbt tests for uniqueness, relationships, accepted values, and score ranges.
5. Great Expectations-style suite for external quality runners.
