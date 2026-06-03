select tenant_id, customer_id, customer_risk_band, max_risk_score, latest_decision
from {{ ref('dim_customers') }}
where customer_risk_band in ('HIGH', 'CRITICAL')
order by max_risk_score desc
