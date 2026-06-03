# Data dictionary

## Silver `kyc_events`

| Column | Meaning |
| --- | --- |
| `event_id` | Unique immutable event id. |
| `tenant_id` | Client/tenant boundary. |
| `customer_id` | KYC customer id. |
| `event_type` | Lifecycle event type. |
| `occurred_at` | Business time. |
| `produced_at` | Producer time. |
| `risk_score` | Score between 0 and 100. |
| `risk_band` | LOW/MEDIUM/HIGH. |
| `is_valid` | Row-level quality status. |
| `quality_error` | Validation failure reason. |

## Mart `dim_customers`

| Column | Meaning |
| --- | --- |
| `customer_sk` | Surrogate key from tenant/customer. |
| `customer_risk_band` | Derived current customer risk band. |
| `has_confirmed_watchlist_match` | Risk flag. |
| `has_rejected_document` | Risk flag. |
| `has_failed_identity_verification` | Risk flag. |
