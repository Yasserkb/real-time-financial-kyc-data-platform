{% snapshot snp_customer_risk_profile %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_sk',
      strategy='check',
      check_cols=['customer_risk_band', 'max_risk_score', 'latest_decision']
    )
}}

select * from {{ ref('dim_customers') }}

{% endsnapshot %}
