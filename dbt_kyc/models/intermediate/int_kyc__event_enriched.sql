with events as (
    select * from {{ ref('stg_kyc__events') }}
),

enriched as (
    select
        *,
        case
            when screening_status = 'CONFIRMED_MATCH' then 100
            when risk_score is not null then risk_score
            when verification_status = 'FAILED' then 85
            when control_status = 'REJECTED' then 65
            else 10
        end as derived_risk_score,
        screening_status = 'CONFIRMED_MATCH' as has_confirmed_watchlist_match,
        control_status = 'REJECTED' as has_rejected_document,
        verification_status = 'FAILED' as has_failed_identity_verification
    from events
    where is_valid
)

select * from enriched
