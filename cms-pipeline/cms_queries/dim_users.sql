SELECT 
    id,
    created_at,
    updated_at,
    email,
    phone,
    full_name,
    gender,
    birth_place,
    birth_date,
    is_superadmin
FROM 
    {dataset_prefix}_bronze.users
WHERE
    is_active = 1;