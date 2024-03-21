from typing import List

from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq

DEFAULT_COLUMNS = [
    "id", 
    "is_active", 
    "created_at",
    "created_by", 
    "updated_at",
    "updated_by", 
    "deleted_at",
    "deleted_by"
]


def cms_to_bronze(
        bigquery: BigQueryResource,
        source_table: str = None,
        dest_table: str = None,
        add_cols: List[str] = []
    ) -> None:
    if source_table and dest_table:
        job_config = bq.QueryJobConfig(
            destination=f"telmark-gcp.cms_dev_bronze.{dest_table}"
        )
        job_config.write_disposition = "WRITE_TRUNCATE"
        columns = DEFAULT_COLUMNS + add_cols
        sql = f"""
            SELECT * FROM
                EXTERNAL_QUERY(
                    'telmark-gcp.asia-southeast2.cms-db-development', 
                    'SELECT {",".join(columns)} FROM {source_table};'
                )
            """

        with bigquery.get_client() as client:
            job = client.query(sql, job_config=job_config)
            job.result()

