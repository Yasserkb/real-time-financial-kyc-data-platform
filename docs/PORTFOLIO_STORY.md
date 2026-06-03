# Portfolio story

## 30-second pitch

I built a real-time KYC data platform inspired by banking onboarding workflows. It ingests lifecycle events through Kafka, processes them with Spark Structured Streaming into Bronze and Silver layers, then builds customer and tenant risk marts with Spark and dbt. I included data contracts, quality gates, orchestration, CI, and cloud-ready Terraform skeletons to make the project look like a production data platform rather than a tutorial.

## Discussion points

- Exactly-once vs effectively-once semantics.
- Late-arriving events and event-time processing.
- Schema evolution with stable envelope and versioned payload.
- Auditability through invalid-row preservation.
- Tenant isolation in a multi-client KYC environment.
