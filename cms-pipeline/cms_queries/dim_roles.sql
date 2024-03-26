SELECT
    unique_id AS users_roles_unique_id,
    roles.company_id,
    users_roles.user_id,
    managers.manager_id AS manager_id,
    roles.name AS role_name,
    roles.is_agent AS is_agent
FROM
    {dataset_prefix}_bronze.users_roles users_roles
        LEFT JOIN {dataset_prefix}_bronze.roles roles
        ON users_roles.role_id = roles.id
            AND users_roles.company_id = roles.company_id
        LEFT JOIN {dataset_prefix}_bronze.companies companies
        ON users_roles.company_id = companies.id
        LEFT JOIN {dataset_prefix}_bronze.companies_users_managers managers
        ON users_roles.company_id = managers.company_id
            AND users_roles.user_id = managers.user_id
WHERE
    users_roles.is_active = 1;