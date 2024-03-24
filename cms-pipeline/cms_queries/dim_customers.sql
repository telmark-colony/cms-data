SELECT
    campaign_customers.customer_id id,
    campaign_customers.campaign_id,
    customers.company_id,
    customers.name,
    customers.phone_number phone_number,
    customers.loan_amount loan_amount,
    customers.outstanding_amount outstanding_amount,
    customers.due_date due_date,
    campaign_customers.status status
FROM
    {dataset_prefix}_bronze.campaign_customers campaign_customers
        INNER JOIN {dataset_prefix}_bronze.customers customers
        ON campaign_customers.customer_id = customers.id