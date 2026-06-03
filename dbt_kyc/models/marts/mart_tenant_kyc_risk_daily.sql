with events as (
    select * from {{ ref('fact_kyc_events') }}
)

select
    tenant_id,
    event_date,
    count(*) as event_count,
    count(distinct customer_id) as active_customers,
    sum(case when event_type = 'CUSTOMER_ONBOARDED' then 1 else 0 end) as onboarding_count,
    sum(case when control_status = 'REJECTED' then 1 else 0 end) as rejected_documents,
    sum(case when screening_status = 'POTENTIAL_MATCH' then 1 else 0 end) as potential_watchlist_matches,
    sum(case when screening_status = 'CONFIRMED_MATCH' then 1 else 0 end) as confirmed_watchlist_matches,
    avg(derived_risk_score) as avg_derived_risk_score,
    max(derived_risk_score) as max_derived_risk_score
from events
group by 1, 2
