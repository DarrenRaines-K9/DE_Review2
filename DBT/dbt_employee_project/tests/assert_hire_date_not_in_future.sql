-- Singular test: ensure no hire dates are in the future
-- Returns zero rows if the test passes

SELECT
    id,
    name,
    hire_date
FROM {{ ref('cleaned_employees') }}
WHERE hire_date > CURRENT_DATE
