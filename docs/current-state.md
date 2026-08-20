# Current state

The repository implements synthetic versioned KYC events, a Redpanda producer, Spark Bronze/Silver/Gold processing, Delta checkpoints, dbt models/tests, Great Expectations configuration, Airflow control-plane orchestration, local Compose infrastructure, and captured execution evidence. Bronze retains raw Kafka metadata. Silver normalizes records, applies event-time watermarking, deduplicates by tenant/event ID, and writes invalid rows to a separate quarantine Delta path.

The local path is the primary reproducible demonstration. AWS S3 and Snowflake Terraform are extension templates and are not presented as a deployed production environment. All sample identities are synthetic.
