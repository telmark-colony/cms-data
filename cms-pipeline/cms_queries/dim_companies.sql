SELECT
    id,
    created_at,
    updated_at,
    name,
    type,
    client_type,
    project_name
FROM
    {dataset_prefix}_bronze.companies
WHERE
    is_active = 1;