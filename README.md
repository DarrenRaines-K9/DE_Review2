# DE Review2

Second Self Assessment for Data Engineering

## DuckDB: ✅

- Create a duckdb, connect with datagrip, add a schema, add a table, insert some fake data, query it.

## S3: ✅

- Manually add a file to s3, read it into memory using polars or pandas, and then write the data to dckdb (no temporary files, do not pull the data to a local file then upload, the data must stay in memory. Use a small file)

## DBT: ✅

- Create a new dbt project in a repo
- Find the profiles.yml file
- Create an external table using a local file
- Create dbt tests (both in schema and in the tests folder)
- Create a model that cleans the data that is in the external table.

## API: ✅

- Get data from an api and save it to s3, directly, do not write it to a file first and then transfer it, hold it in memory and write it to a file in s3.

## Github: ✅

- Be able to complete the following workflow
- Create a local branch based off of main
- Make an update to the branch
- Push that branch to github
- Create a pull request to main
- Approve the PR
- Switch you local branch to main
- Pull main
- Check out the local branch that you created in step 1
- Merge main into that branch

## Docker: ✅

- Spin up postgres
- Connect to it datagrip
- Create a schema
- Create a table
- Add data
- Create a dev container
- Add the aws cli feature
- Mount the folder that you need for sso
- Verify that you can run sso inside of the dev container
- Since Minio is in maintenance mode, look for a container image for rustFS and follow the directions to spin it up locally, add a file to it and query the file from psotgres

## Snowflake: ✅

- Create a user called dbt user, and add permissions to that user to that it can be used to connect to snowflake
- In a python folder create a new dbt project and use the dbt snowflake adapter to connect to snowflake. 
- You will test this by running dbt debug in the project and seeing all green checks. 
