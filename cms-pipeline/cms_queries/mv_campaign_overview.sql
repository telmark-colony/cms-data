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
        campaign_id,
        campaign_name,
        campaign_status
    FROM
        {dataset_prefix}_silver.dim_user_view
) user_view
LEFT JOIN {dataset_prefix}_silver.dim_customers customers
ON user_view.campaign_id = customers.campaign_id