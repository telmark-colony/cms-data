WITH user_base AS (
    SELECT
        users.id as user_id,
        users.email,
        users.full_name,
        users.is_superadmin,
        roles.manager_id,
        companies.id as company_id,
        companies.name as company_name,
        roles.users_roles_unique_id as role_id,
        roles.role_name,
        roles.is_agent,
        campaigns.id as campaign_id,
        campaigns.name as campaign_name,
        campaigns.latest_status as campaign_status,
        campaigns.type as campaign_type
    FROM
        {dataset_prefix}_silver.dim_roles roles
            INNER JOIN {dataset_prefix}_silver.dim_users users
                ON roles.user_id = users.id
            INNER JOIN {dataset_prefix}_silver.dim_companies companies
                ON roles.company_id = companies.id
            INNER JOIN {dataset_prefix}_silver.dim_campaigns campaigns
                ON companies.id = campaigns.company_id
)
SELECT 
    user_view_as_id,
    user_view_as_email,
    user_view_is_agent,
    user_id,
    manager_id,
    email,
    full_name,
    is_superadmin,
    company_id,
    company_name,
    role_id,
    role_name,
    is_agent,
    campaign_id,
    campaign_name,
    campaign_status,
    campaign_type
FROM (
    (
        SELECT 
            user_id user_view_as_id, 
            email user_view_as_email,
            is_agent user_view_is_agent,
            user_base.* 
        FROM 
            user_base
    ) 
    UNION ALL
    (
        SELECT 
            user_base.manager_id user_view_as_id, 
            manager.email user_view_as_email,
            manager_roles.is_agent user_view_is_agent,
            user_base.* 
        FROM 
            user_base 
                LEFT JOIN {dataset_prefix}_silver.dim_users manager
                    ON user_base.manager_id = manager.id
                LEFT JOIN {dataset_prefix}_silver.dim_roles manager_roles
                    ON user_base.manager_id = manager_roles.user_id 
                    AND user_base.company_id = manager_roles.company_id
        WHERE 
            user_base.manager_id IS NOT NULL
    ) 
) t_all