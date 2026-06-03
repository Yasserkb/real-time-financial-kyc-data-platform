with events as (
    select * from {{ ref('int_kyc__event_enriched') }}
),

customer_rollup as (
    select
        tenant_id,
        customer_id,
        min(case when event_type = 'CUSTOMER_ONBOARDED' then occurred_at end) as onboarded_at,
        max(occurred_at) as last_event_at,
        count(*) as event_count,
        max(derived_risk_score) as max_risk_score,
        max(case when has_confirmed_watchlist_match then 1 else 0 end) as has_confirmed_watchlist_match,
        max(case when has_rejected_document then 1 else 0 end) as has_rejected_document,
        max(case when has_failed_identity_verification then 1 else 0 end) as has_failed_identity_verification,
        max(case when decision is not null then decision end) as latest_decision
    from events
    group by 1, 2
)

select * from customer_rollup
