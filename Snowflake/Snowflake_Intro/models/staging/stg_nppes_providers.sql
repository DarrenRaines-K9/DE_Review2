{{
    config(
        materialized='view'
    )
}}

-- Staging model: Clean and standardize NPPES provider data
-- This layer focuses on renaming, type casting, and basic cleanup

with source as (
    select * from {{ source('raw', 'nppes_sample') }}
),

cleaned as (
    select
        -- Primary identifier
        "NPI" as provider_npi,

        -- Entity information
        "Entity_Type_Code" as entity_type_code,
        case
            when "Entity_Type_Code" = 1 then 'Individual'
            when "Entity_Type_Code" = 2 then 'Organization'
            else 'Unknown'
        end as entity_type,

        -- Individual provider names
        "Provider_Last_Name_Legal_Name" as provider_last_name,
        "Provider_First_Name" as provider_first_name,
        "Provider_Middle_Name" as provider_middle_name,
        "Provider_Credential_Text" as provider_credentials,

        -- Organization name
        "Provider_Organization_Name_Legal_Business_Name" as organization_name,

        -- Practice location
        "Provider_First_Line_Business_Practice_Location_Address" as practice_address_line1,
        "Provider_Second_Line_Business_Practice_Location_Address" as practice_address_line2,
        "Provider_Business_Practice_Location_Address_City_Name" as practice_city,
        "Provider_Business_Practice_Location_Address_State_Name" as practice_state,
        "Provider_Business_Practice_Location_Address_Postal_Code" as practice_zip,
        "Provider_Business_Practice_Location_Address_Telephone_Number" as practice_phone,

        -- Mailing address
        "Provider_First_Line_Business_Mailing_Address" as mailing_address_line1,
        "Provider_Business_Mailing_Address_City_Name" as mailing_city,
        "Provider_Business_Mailing_Address_State_Name" as mailing_state,
        "Provider_Business_Mailing_Address_Postal_Code" as mailing_zip,

        -- Taxonomy (specialty)
        "Healthcare_Provider_Taxonomy_Code_1" as primary_taxonomy_code,
        "Healthcare_Provider_Primary_Taxonomy_Switch_1" as is_primary_taxonomy,

        -- Demographics
        "Provider_Sex_Code" as gender,

        -- Dates
        "Provider_Enumeration_Date" as enumeration_date,
        "Last_Update_Date" as last_update_date,

        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source
)

select * from cleaned
