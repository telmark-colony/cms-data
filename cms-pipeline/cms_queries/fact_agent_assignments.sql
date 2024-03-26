SELECT
    campaigns.company_id,
    flow.campaign_id,
    agent_customer.campaign_flow_id,
    agent_customer.agent_id,
    agent_customer.agent_status,
    agent_customer.customer_id,
    agent_customer.customer_status,
    agent_customer.expiration_time,
    agent_customer.total_unread,
    agent_customer.last_interact_time
FROM (
    SELECT
        COALESCE(flow_agents.campaign_flow_id, flow_customers.campaign_flow_id) campaign_flow_id,
        COALESCE(flow_agents.user_id, flow_customers.user_id) agent_id,
        flow_agents.status agent_status,
        flow_customers.customer_id customer_id,
        flow_customers.status customer_status,
        flow_customers.expiration_time,
        flow_customers.total_unread,
        flow_customers.last_interact_time
    FROM
        (SELECT * FROM {dataset_prefix}_bronze.campaign_flow_agents WHERE is_active = 1) flow_agents
            FULL JOIN (SELECT * FROM {dataset_prefix}_bronze.campaign_flow_customers WHERE is_active = 1) flow_customers
                ON flow_agents.user_id = flow_customers.user_id
                AND flow_agents.campaign_flow_id = flow_customers.campaign_flow_id
) agent_customer
    LEFT JOIN {dataset_prefix}_bronze.campaign_flows flow
        ON agent_customer.campaign_flow_id = flow.id
    LEFT JOIN {dataset_prefix}_bronze.campaigns campaigns
        ON flow.campaign_id = campaigns.id