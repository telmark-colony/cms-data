from dagster import asset

from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq

from .cms_utils import cms_to_bronze, execute_sql_file


@asset
def cms_campaign_flow_agents(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="campaign_flow_agents",
        dest_table="campaign_flow_agents",
        add_cols=[
            "campaign_flow_id",
            "user_id",
            "status"
        ]
    )


@asset(deps=[cms_campaign_flow_agents])
def mv_agent_status(bigquery: BigQueryResource) -> None:
    execute_sql_file(bigquery, "gold", "mv_agent_status")