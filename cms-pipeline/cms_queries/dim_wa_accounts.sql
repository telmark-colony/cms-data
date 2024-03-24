SELECT
    id,
    company_id,
    phone_number,
    active_chat_limit,
    provider_type,
    status,
    wa_phone_number_id
FROM
    {dataset_prefix}_bronze.wa_accounts
WHERE
    is_active = 1