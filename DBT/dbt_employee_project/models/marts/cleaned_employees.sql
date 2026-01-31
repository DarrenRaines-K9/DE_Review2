-- Cleaned employees model
-- Removes nulls, drops duplicates, and trims whitespace

WITH source AS (
    SELECT * FROM {{ ref('stg_employees') }}
),

cleaned AS (
    SELECT
        id,
        TRIM(name) AS name,
        TRIM(department) AS department,
        salary,
        CAST(hire_date AS DATE) AS hire_date
    FROM source
    WHERE
        id IS NOT NULL
        AND name IS NOT NULL
        AND department IS NOT NULL
        AND salary IS NOT NULL
        AND hire_date IS NOT NULL
),

deduplicated AS (
    SELECT DISTINCT *
    FROM cleaned
)

SELECT * FROM deduplicated
