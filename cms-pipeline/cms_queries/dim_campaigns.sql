 SELECT
    campaigns.id,      
    campaigns.company_id,
    campaigns.name,
    campaigns.description,
    campaigns.start_date,
    campaigns.end_date,
    campaigns.latest_status,
    campaigns.daily_start_time,
    campaigns.daily_end_time,
    wa_campaign_details.type,
    wa_campaign_details.provider_type wa_provider_type,
    wa_campaign_details.id details_id
FROM
    {dataset_prefix}_bronze.campaigns campaigns
        LEFT JOIN {dataset_prefix}_bronze.wa_campaign_details wa_campaign_details
        ON campaigns.id = wa_campaign_details.campaign_id
WHERE
    campaigns.is_active = 1 AND wa_campaign_details.is_active = 1;