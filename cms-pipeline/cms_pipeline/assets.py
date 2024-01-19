from dagster import asset

from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq

@asset
def users(bigquery: BigQueryResource) -> None:
    job_config = bq.QueryJobConfig(destination="telmark-gcp.cms_dev_bronze.users")
    sql = "SELECT * FROM EXTERNAL_QUERY('telmark-gcp.asia-southeast2.cms-db-development', 'SELECT * FROM users;')"

    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()