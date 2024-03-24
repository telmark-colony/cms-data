SELECT
    wa_chats.wa_message_id,
    wa_chats.wa_chat_room_id,
    wa_chat_rooms.wa_account_id,
    wa_chat_rooms.phone_number,
    wa_chat_rooms.unread_replies,
    wa_chat_rooms.room_expiration_time,
    wa_chats.is_self,
    wa_chats.message,
    wa_chats.status,
    wa_chats.sent_timestamp,
    wa_chats.delivered_timestamp,
    wa_chats.read_timestamp,
    wa_chats.wa_campaign_detail_id,
    wa_chats.user_id,
    users.email user_email
FROM
    {dataset_prefix}_bronze.wa_chats wa_chats
    LEFT JOIN {dataset_prefix}_bronze.users users
        ON wa_chats.user_id = users.id
    LEFT JOIN {dataset_prefix}_bronze.wa_chat_rooms wa_chat_rooms
        ON wa_chats.wa_chat_room_id = wa_chat_rooms.id
WHERE
    wa_chats.is_active = 1;