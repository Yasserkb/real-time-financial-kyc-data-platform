select event_id
from {{ ref('fact_kyc_events') }}
where derived_risk_score < 0 or derived_risk_score > 100
