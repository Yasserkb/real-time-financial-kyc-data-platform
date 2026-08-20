# Known limitations

- Local filesystem Delta paths are the default; MinIO/S3 wiring requires a production profile and credentials.
- The project demonstrates checkpointed, idempotent processing but does not claim exactly-once behavior across every external system.
- Great Expectations configuration exists, but automated quarantine replay is not yet implemented.
- Infrastructure templates are not evidence of a live Snowflake or AWS deployment.
- Benchmark numbers must be generated on named hardware; no unmeasured throughput claim is made.
