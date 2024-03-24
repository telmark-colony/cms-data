import os
from pathlib import Path
from typing import List

from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq


PROJECT_NAME = os.getenv("PROJECT_NAME")
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATASET_PREFIX = os.getenv("DATASET_PREFIX")

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
            destination=f"{PROJECT_NAME}.{DATASET_PREFIX}_bronze.{dest_table}"
        )
        job_config.write_disposition = "WRITE_TRUNCATE"
        columns = DEFAULT_COLUMNS + add_cols
        sql = f"""
            SELECT * FROM
                EXTERNAL_QUERY(
                    '{PROJECT_NAME}.asia-southeast2.{DATABASE_NAME}', 
                    'SELECT {",".join(columns)} FROM {source_table};'
                )
            """

        with bigquery.get_client() as client:
            job = client.query(sql, job_config=job_config)
            job.result()


def execute_sql_file(bigquery: BigQueryResource, stage: str, table_name: str) -> None:
    job_config = bq.QueryJobConfig(
        destination=f"{PROJECT_NAME}.{DATASET_PREFIX}_{stage}.{table_name}"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    template_query = Path(f"cms_queries/{table_name}.sql").read_text()
    query = template_query.format(dataset_prefix=DATASET_PREFIX)
    with bigquery.get_client() as client:
        job = client.query(query, job_config=job_config)
        job.result()