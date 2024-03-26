SELECT
    user_campaign.user_view_as_id,
    user_campaign.user_view_as_email,
    user_campaign.user_view_is_agent,
    user_campaign.user_id,
    user_campaign.email,
    user_campaign.full_name,
    user_campaign.is_agent,
    user_campaign.company_id,
    user_campaign.company_name,
    user_campaign.campaign_id,
    user_campaign.campaign_name,
    user_campaign.campaign_status,
    user_campaign.campaign_type,
    user_message.num_sent,
    user_message.num_delivered,
    user_message.num_read,
    reply_message.num_reply,
FROM (
    SELECT 
        user_view.user_view_as_id,
        user_view.user_view_as_email,
        user_view.user_view_is_agent,
        user_view.user_id,
        user_view.email,
        user_view.full_name,
        user_view.is_agent,
        user_view.company_id,
        user_view.company_name,
        user_view.campaign_id,
        user_view.campaign_name,
        user_view.campaign_status,
        user_view.campaign_type,
        campaigns.details_id
    FROM
        {dataset_prefix}_silver.dim_user_view user_view
            LEFT JOIN {dataset_prefix}_silver.dim_campaigns campaigns
            ON user_view.campaign_id = campaigns.id
) user_campaign 
LEFT JOIN (
    SELECT
        wa_campaign_detail_id,
        user_id,
        COUNT(sent_timestamp) num_sent,
        SUM(CASE WHEN sent_timestamp IS NOT NULL AND delivered_timestamp IS NOT NULL THEN 1 ELSE 0 END) num_delivered,
        SUM(CASE WHEN sent_timestamp IS NOT NULL AND delivered_timestamp IS NOT NULL AND read_timestamp IS NOT NULL THEN 1 ELSE 0 END) num_read
    FROM
        {dataset_prefix}_silver.dim_waba_chat
    WHERE
        user_id IS NOT NULL
    GROUP BY
        1, 2
) user_message
    ON user_campaign.details_id = user_message.wa_campaign_detail_id
    AND user_campaign.user_id = user_message.user_id
LEFT JOIN (
    SELECT
        wa_campaign_detail_id,
        user_id,
        SUM(CASE WHEN has_reply = 0 THEN 1 ELSE 0 END) num_reply
    FROM (
        SELECT
            wa_chat_room_id,
            MAX(wa_campaign_detail_id) wa_campaign_detail_id,
            MAX(user_id) user_id,
            MIN(is_self) has_reply
        FROM
            {dataset_prefix}_silver.dim_waba_chat
        GROUP BY 1
    ) t_waba1
    GROUP BY 1, 2
) reply_message
ON user_message.wa_campaign_detail_id = reply_message.wa_campaign_detail_id
AND user_message.user_id = reply_message.user_id