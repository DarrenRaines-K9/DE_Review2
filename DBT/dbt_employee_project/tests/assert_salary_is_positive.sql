-- Singular test: ensure all salaries are positive numbers
-- Returns zero rows if the test passes

SELECT
    id,
    name,
    salary
FROM {{ ref('cleaned_employees') }}
WHERE salary <= 0
