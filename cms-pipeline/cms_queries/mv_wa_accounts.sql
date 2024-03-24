SELECT
    user_view.user_view_as_id,
    user_view.user_view_as_email,
    user_view.user_id,
    user_view.full_name,
    user_view.email,
    user_view.company_id,
    user_view.company_name,
    wa_accounts.id wa_account_id,
    wa_accounts.wa_phone_number_id,
    wa_accounts.phone_number,
    wa_accounts.active_chat_limit,
    wa_accounts.provider_type,
    wa_accounts.status
FROM (
    SELECT DISTINCT
        user_view_as_id,
        user_view_as_email,
        user_id,
        full_name,
        email,
        company_id,
        company_name
    FROM
        {dataset_prefix}_silver.dim_user_view
) user_view
LEFT JOIN {dataset_prefix}_silver.dim_wa_accounts wa_accounts
ON user_view.company_id = wa_accounts.company_id