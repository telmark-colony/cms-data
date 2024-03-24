SELECT
    id,
    company_id,
    type,
    provider_type,
    name,
    content,
    file_key,
    status,
    meta_template_name,
    rejected_reason
FROM
    {dataset_prefix}_bronze.wa_templates
WHERE
    is_active = 1