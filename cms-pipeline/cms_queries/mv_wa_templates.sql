SELECT
    user_view.user_view_as_id,
    user_view.user_view_as_email,
    user_view.user_id,
    user_view.full_name,
    user_view.email,
    user_view.company_id,
    user_view.company_name,
    wa_templates.id wa_template_id,
    wa_templates.type wa_template_type,
    wa_templates.provider_type wa_template_provider_type,
    wa_templates.name wa_template_name,
    wa_templates.status wa_template_status,
    wa_templates.meta_template_name wa_meta_template_name,
    wa_templates.rejected_reason wa_template_rejected_reason
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
LEFT JOIN {dataset_prefix}_silver.dim_wa_templates wa_templates
ON user_view.company_id = wa_templates.company_id