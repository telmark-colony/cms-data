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
        {dataset_prefix}_silver.dim_user_view user_view
) user_view
LEFT JOIN (
    SELECT DISTINCT
        id,
        campaign_id,
        name,
        step
    FROM
        {dataset_prefix}_bronze.campaign_flows
) campaign_flows
    ON user_view.campaign_id = campaign_flows.campaign_id
INNER JOIN (
    SELECT DISTINCT
        user_id,
        campaign_flow_id,
        status
    FROM
        {dataset_prefix}_bronze.campaign_flow_agents
) campaign_flow_agents
    ON user_view.user_id = campaign_flow_agents.user_id
    AND campaign_flows.id = campaign_flow_agents.campaign_flow_id