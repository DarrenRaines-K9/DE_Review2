{{
    config(
        materialized='table'
    )
}}

-- Analytics-ready dimension table for providers
-- This is the final layer for BI tools and analytics

with staged_providers as (
    select * from {{ ref('stg_nppes_providers') }}
),

final as (
    select
        provider_npi,
        entity_type,

        -- Full name for individuals
        case
            when entity_type = 'Individual'
            then trim(
                coalesce(provider_first_name, '') || ' ' ||
                coalesce(provider_middle_name, '') || ' ' ||
                coalesce(provider_last_name, '')
            )
            else organization_name
        end as provider_full_name,

        provider_credentials,

        -- Location information
        practice_address_line1,
        practice_city,
        practice_state,
        practice_zip,
        practice_phone,

        -- Demographics
        gender,

        -- Specialty
        primary_taxonomy_code,

        -- Dates
        enumeration_date,
        last_update_date,
        dbt_loaded_at

    from staged_providers

    -- Filter out incomplete records
    where provider_npi is not null
)

select * from final
