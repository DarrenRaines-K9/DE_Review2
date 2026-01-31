-- Staging model: reads raw data from local CSV file
-- This acts as our external table using DuckDB's read_csv_auto()

SELECT
    id,
    name,
    department,
    salary,
    hire_date
FROM read_csv_auto('/workspaces/app/sample_data.csv')
