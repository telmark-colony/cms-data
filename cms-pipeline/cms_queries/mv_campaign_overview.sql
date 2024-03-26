SELECT
    user_view_as_id,
    user_view_as_email,
    campaign_id,
    campaign_name,
    campaign_status,
    customer_id,
    customer_name,
    customer_phone_number,
    customer_loan_amount,
    customer_outstanding_amount,
    customer_status
FROM (
    -- when the user_view is an agent: only see customers assigned to the user_view
    SELECT
        user_view.user_view_as_id,
        user_view.user_view_as_email,
        user_view.campaign_id,
        user_view.campaign_name,
        user_view.campaign_status,
        customers.id customer_id,
        customers.name customer_name,
        customers.phone_number customer_phone_number,
        customers.loan_amount customer_loan_amount,
        customers.outstanding_amount customer_outstanding_amount,
        customers.status customer_status
    FROM (
        SELECT DISTINCT
            user_view_as_id,
            user_view_as_email,
            user_view_is_agent,
            user_id,
            company_id,
            campaign_id,
            campaign_name,
            campaign_status
        FROM
            {dataset_prefix}_silver.dim_user_view
        WHERE
            user_view_is_agent = 1
    ) user_view
    LEFT JOIN (
        SELECT DISTINCT
            agent_id,
            company_id,
            campaign_id,
            customer_id
        FROM 
            {dataset_prefix}_silver.fact_agent_assignments 
    ) agent_customer
        ON user_view.user_id = agent_customer.agent_id
        AND user_view.campaign_id = agent_customer.campaign_id
    LEFT JOIN {dataset_prefix}_silver.dim_customers customers
        ON agent_customer.customer_id = customers.id
        AND agent_customer.campaign_id = customers.campaign_id
)
UNION ALL (
    -- otherwise, user_view can see all customers within a campaign
    SELECT
        user_view.user_view_as_id,
        user_view.user_view_as_email,
        user_view.campaign_id,
        user_view.campaign_name,
        user_view.campaign_status,
        customers.id customer_id,
        customers.name customer_name,
        customers.phone_number customer_phone_number,
        customers.loan_amount customer_loan_amount,
        customers.outstanding_amount customer_outstanding_amount,
        customers.status customer_status
    FROM (
        SELECT DISTINCT
            user_view_as_id,
            user_view_as_email,
            user_view_is_agent,
            campaign_id,
            campaign_name,
            campaign_status
        FROM
            {dataset_prefix}_silver.dim_user_view
        WHERE
            user_view_is_agent = 0
    ) user_view
    LEFT JOIN {dataset_prefix}_silver.dim_customers customers
    ON user_view.campaign_id = customers.campaign_id
)
