# Ingestion layer

Owns event contracts and producers.

- `schemas/`: JSON Schema + Avro event contract.
- `producers/event_factory.py`: synthetic KYC customer journeys.
- `producers/kyc_event_producer.py`: Kafka-compatible producer.
