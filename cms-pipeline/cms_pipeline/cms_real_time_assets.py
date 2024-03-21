from dagster import asset

from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq

from .cms_utils import cms_to_bronze


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
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_gold.mv_agent_status"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = """
    SELECT
        user_view.user_view_as_id,
        user_view.user_view_as_email,
        user_view.user_id,
        user_view.full_name,
        user_view.email,
        user_view.campaign_id,
        user_view.campaign_name,
        user_view.campaign_status,
        campaign_flows.name campaign_flow_name,
        campaign_flows.step campaign_flow_step,
        campaign_flow_agents.status
    FROM (
        SELECT DISTINCT
            user_view_as_id,
            user_view_as_email,
            user_id,
            full_name,
            email,
            campaign_id,
            campaign_name,
            campaign_status
        FROM
            cms_dev_silver.dim_user_view user_view
    ) user_view
    LEFT JOIN (
        SELECT DISTINCT
            id,
            campaign_id,
            name,
            step
        FROM
            cms_dev_bronze.campaign_flows
    ) campaign_flows
        ON user_view.campaign_id = campaign_flows.campaign_id
    INNER JOIN (
        SELECT DISTINCT
            user_id,
            campaign_flow_id,
            status
        FROM
            cms_dev_bronze.campaign_flow_agents
    ) campaign_flow_agents
        ON user_view.user_id = campaign_flow_agents.user_id
        AND campaign_flows.id = campaign_flow_agents.campaign_flow_id
    """
    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()