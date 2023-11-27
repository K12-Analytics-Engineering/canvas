# Canvas Data 2

An open source repository for pulling Canvas Data 2 data into a dbt project.

## Startup Steps

This project assumes that you will use Google Cloud Storage (GCS) for external file storage and BigQuery for the database layer. Before beginning, you will need to:

- create a gcloud project
- create a service account, and download a credentials file
- set up the [gcloud cli](https://cloud.google.com/sdk/docs/install)

Once you have your Google Cloud environment set up, you can:

1. Create a GCS bucket. Within your bucket, create a folder called `canvasdata2_api`. Grant `Storage Legacy Bucket Reader` and `Storage Object Admin` access to your service account.
1. Set all environment variables and save to `.env`. See the `.env-sample` file for a template. __DO NOT COMMIT SENSITIVE DATA TO GIT__
1. Install a Python virtual environment using `poetry install`.
1. Install dbt packages using `dbt deps`.
1. There is a 'chicken and egg' problem in using dagster to view dbt assets the first time, since the dbt assets rely on Canvas data that has not been created yet. To load some initial data, run `DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagster dev -f dagster/repository.py` and execute the `all_canvasdata2_job` job.
1. Once you have successfully loaded your first Canvas data, exit the dagster webserver and build your dbt project using `dbt run-operation stage_external_sources` and then `dbt build`.
1. Enter the dagster webserver again (`dagster dev -f dagster/repository.py`), and you should now see your assets.
