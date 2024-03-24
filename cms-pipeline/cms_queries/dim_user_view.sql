WITH user_base AS (
    SELECT
        users.id as user_id,
        roles.manager_id,
        users.email,
        users.full_name,
        users.is_superadmin,
        companies.id as company_id,
        companies.name as company_name,
        roles.users_roles_unique_id as role_id,
        roles.role_name,
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
    user_id,
    manager_id,
    email,
    full_name,
    is_superadmin,
    company_id,
    company_name,
    role_id,
    role_name,
    campaign_id,
    campaign_name,
    campaign_status,
    campaign_type
FROM (
    (SELECT user_id user_view_as_id, * FROM user_base) 
    UNION ALL
    (SELECT manager_id user_view_as_id, * FROM user_base WHERE manager_id IS NOT NULL) 
) t_all
LEFT JOIN (
    SELECT
        id,
        email user_view_as_email
    FROM
        {dataset_prefix}_silver.dim_users
) t_users
ON t_all.user_view_as_id = t_users.id