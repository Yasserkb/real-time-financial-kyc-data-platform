# Data contracts

## Event envelope

| Field | Required | Description |
| --- | --- | --- |
| `event_id` | yes | Immutable event id. |
| `event_type` | yes | KYC lifecycle event type. |
| `event_version` | yes | Contract version. |
| `tenant_id` | yes | Tenant/client boundary. |
| `customer_id` | yes | Stable customer id. |
| `occurred_at` | yes | Business event timestamp. |
| `produced_at` | yes | Producer emission timestamp. |
| `payload` | yes | Event-specific JSON object. |
| `metadata` | yes | Producer and trace metadata. |

## Compatibility rules

- Never remove envelope fields.
- Additive payload fields are allowed.
- Breaking changes require a new `event_version`.
- Consumers should ignore unknown payload fields.
