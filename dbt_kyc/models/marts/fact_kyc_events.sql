with events as (
    select * from {{ ref('int_kyc__event_enriched') }}
)

select
    event_id,
    {{ dbt_utils.generate_surrogate_key(['tenant_id', 'customer_id']) }} as customer_sk,
    tenant_id,
    customer_id,
    event_type,
    event_version,
    occurred_at,
    produced_at,
    event_date,
    document_type,
    control_status,
    verification_status,
    screening_status,
    risk_score,
    risk_band,
    decision,
    derived_risk_score,
    has_confirmed_watchlist_match,
    has_rejected_document,
    has_failed_identity_verification
from events
