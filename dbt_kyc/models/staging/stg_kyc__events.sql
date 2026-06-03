with source_events as (
    select * from {{ source('raw_kyc', 'silver_kyc_events') }}
),

typed as (
    select
        cast(event_id as {{ dbt.type_string() }}) as event_id,
        cast(event_type as {{ dbt.type_string() }}) as event_type,
        cast(event_version as integer) as event_version,
        cast(tenant_id as {{ dbt.type_string() }}) as tenant_id,
        cast(customer_id as {{ dbt.type_string() }}) as customer_id,
        cast(occurred_at as timestamp) as occurred_at,
        cast(produced_at as timestamp) as produced_at,
        nullif(cast(document_type as {{ dbt.type_string() }}), '') as document_type,
        nullif(cast(control_status as {{ dbt.type_string() }}), '') as control_status,
        nullif(cast(verification_status as {{ dbt.type_string() }}), '') as verification_status,
        nullif(cast(screening_status as {{ dbt.type_string() }}), '') as screening_status,
        cast(nullif(cast(risk_score as {{ dbt.type_string() }}), '') as integer) as risk_score,
        nullif(cast(risk_band as {{ dbt.type_string() }}), '') as risk_band,
        nullif(cast(decision as {{ dbt.type_string() }}), '') as decision,
        cast(is_valid as boolean) as is_valid,
        cast(event_date as date) as event_date
    from source_events
)

select * from typed
