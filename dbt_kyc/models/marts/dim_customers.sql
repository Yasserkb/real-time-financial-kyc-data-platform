with customer_rollup as (
    select * from {{ ref('int_kyc__customer_rollup') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['tenant_id', 'customer_id']) }} as customer_sk,
    tenant_id,
    customer_id,
    onboarded_at,
    last_event_at,
    event_count,
    max_risk_score,
    case
        when has_confirmed_watchlist_match = 1 then 'CRITICAL'
        when max_risk_score >= 75 then 'HIGH'
        when max_risk_score >= 40 then 'MEDIUM'
        else 'LOW'
    end as customer_risk_band,
    cast(has_confirmed_watchlist_match as boolean) as has_confirmed_watchlist_match,
    cast(has_rejected_document as boolean) as has_rejected_document,
    cast(has_failed_identity_verification as boolean) as has_failed_identity_verification,
    latest_decision,
    current_timestamp as dbt_loaded_at
from customer_rollup
