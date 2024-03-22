from dagster import asset

from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq

from .cms_utils import cms_to_bronze



@asset
def cms_users(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="users",
        dest_table="users",
        add_cols=[
            "email",
            "phone",
            "full_name",
            "gender",
            "birth_place",
            "birth_date",
            "is_superadmin"
        ]
    )


@asset
def cms_users_roles(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="users_roles",
        dest_table="users_roles",
        add_cols=[
            "user_id",
            "role_id",
            "company_id",
            "unique_id"
        ]
    )

@asset
def cms_roles(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="roles",
        dest_table="roles",
        add_cols=[
            "company_id",
            "name"
        ]
    )


@asset
def cms_companies_users_managers(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="companies_users_managers",
        dest_table="companies_users_managers",
        add_cols=[
            "company_id",
            "user_id",
            "manager_id"
        ]
    )


@asset
def cms_companies(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="companies",
        dest_table="companies",
        add_cols=[
            "name",
            "type",
            "client_type",
            "project_name"
        ]
    )


@asset
def cms_customers(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="customers",
        dest_table="customers",
        add_cols=[
            "batch_id",
            "loan_id",
            "company_id",
            "company_template_id",
            "name",
            "phone_number",
            "loan_amount",
            "outstanding_amount",
            "due_date"
        ]
    )


@asset
def cms_campaigns(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="campaigns",
        dest_table="campaigns",
        add_cols=[
            "company_id",
            "name",
            "description",
            "start_date",
            "end_date",
            "latest_status",
            "daily_start_time",
            "daily_end_time"
        ]
    )


@asset
def cms_campaign_customers(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="campaign_customers",
        dest_table="campaign_customers",
        add_cols=[
            "campaign_id",
            "customer_id",
            "status",
            "expiration_time",
            "priority"
        ]
    )


@asset
def cms_campaign_flows(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="campaign_flows",
        dest_table="campaign_flows",
        add_cols=[
            "channel_specific_id",
            "name",
            "campaign_id",
            "step",
            "status",
            "start_time",
            "end_time",
            "post_condition",
            "channel_specific_type"
        ]
    )


@asset
def cms_wa_accounts(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="wa_accounts",
        dest_table="wa_accounts",
        add_cols=[
            "company_id",
            "phone_number",
            "active_chat_limit",
            "provider_type",
            "status",
            "wa_phone_number_id"
        ]
    )


@asset
def cms_wa_templates(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="wa_templates",
        dest_table="wa_templates",
        add_cols=[
            "company_id",
            "type",
            "provider_type",
            "name",
            "content",
            "file_key",
            "status",
            "meta_template_name",
            "rejected_reason"
        ]
    )



@asset
def cms_wa_campaign_details(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="wa_campaign_details",
        dest_table="wa_campaign_details",
        add_cols=[
            "campaign_id",
            "wa_template_id",
            "type",
            "provider_type"
        ]
    )


@asset
def cms_wa_chat(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="wa_chats",
        dest_table="wa_chats",
        add_cols=[
            "wa_message_id",
            "wa_chat_room_id",
            "is_self",
            "message",
            "status",
            "sent_timestamp",
            "delivered_timestamp",
            "read_timestamp",
            "wa_campaign_detail_id",
            "user_id",
            "type",
            "failed_reason",
            "raw"
        ]
    )



@asset
def cms_wa_chat_rooms(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="wa_chat_rooms",
        dest_table="wa_chat_rooms",
        add_cols=[
            "wa_account_id",
            "phone_number",
            "unread_replies",
            "room_expiration_time",
            "init_by_self_timestamp"
        ]
    )



@asset
def cms_wa_agent_customer_rooms(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="wa_agent_customer_rooms",
        dest_table="wa_agent_customer_rooms",
        add_cols=[
            "campaign_flow_customer_id",
            "wa_chat_room_id"
        ]
    )


@asset(deps=[cms_users])
def dim_users(bigquery: BigQueryResource) -> None:
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_silver.dim_users"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = f"""
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
            cms_dev_bronze.users
        WHERE
            is_active = 1;
        """

    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()


@asset(
    deps=[
        cms_users_roles,
        cms_roles,
        cms_companies_users_managers
    ]
)
def dim_roles(bigquery: BigQueryResource) -> None:
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_silver.dim_roles"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = f"""
        SELECT
            unique_id AS users_roles_unique_id,
            roles.company_id,
            users_roles.user_id,
            managers.manager_id AS manager_id,
            roles.name AS role_name
        FROM
            cms_dev_bronze.users_roles users_roles
                LEFT JOIN cms_dev_bronze.roles roles
                ON users_roles.role_id = roles.id
                    AND users_roles.company_id = roles.company_id
                LEFT JOIN cms_dev_bronze.companies companies
                ON users_roles.company_id = companies.id
                LEFT JOIN cms_dev_bronze.companies_users_managers managers
                ON users_roles.company_id = managers.company_id
                    AND users_roles.user_id = managers.user_id
        WHERE
            users_roles.is_active = 1;
        """

    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()


@asset(deps=[cms_companies])
def dim_companies(bigquery: BigQueryResource) -> None:
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_silver.dim_companies"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = f"""
        SELECT
            id,
            created_at,
            updated_at,
            name,
            type,
            client_type,
            project_name
        FROM
            cms_dev_bronze.companies
        WHERE
            is_active = 1;
        """

    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()


@asset(deps=[cms_campaigns, cms_wa_campaign_details])
def dim_campaigns(bigquery: BigQueryResource) -> None:
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_silver.dim_campaigns"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = f"""
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
            cms_dev_bronze.campaigns campaigns
                LEFT JOIN cms_dev_bronze.wa_campaign_details wa_campaign_details
                ON campaigns.id = wa_campaign_details.campaign_id
        WHERE
            campaigns.is_active = 1 AND wa_campaign_details.is_active = 1;
        """

    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()


@asset(
    deps=[
        cms_customers, 
        cms_campaign_customers
    ]
)
def dim_customers(bigquery: BigQueryResource):
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_silver.dim_customers"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = """
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
            cms_dev_bronze.campaign_customers campaign_customers
                INNER JOIN cms_dev_bronze.customers customers
                ON campaign_customers.customer_id = customers.id
    """

    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()


@asset(deps=[dim_users, dim_roles])
def dim_user_view(bigquery: BigQueryResource) -> None:
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_silver.dim_user_view"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = f"""
        WITH user_base AS (
            SELECT
                users.id as user_id,
                roles.manager_id,
                users.email,
                users.full_name,
                users.is_superadmin,
                companies.id as company_id,
                companies.name as company_name,
                roles.users_roles_unique_id as role_id,
                roles.role_name,
                campaigns.id as campaign_id,
                campaigns.name as campaign_name,
                campaigns.latest_status as campaign_status,
                campaigns.type as campaign_type
            FROM
                cms_dev_silver.dim_roles roles
                    INNER JOIN cms_dev_silver.dim_users users
                        ON roles.user_id = users.id
                    INNER JOIN cms_dev_silver.dim_companies companies
                        ON roles.company_id = companies.id
                    INNER JOIN cms_dev_silver.dim_campaigns campaigns
                        ON companies.id = campaigns.company_id
        )
        SELECT 
            user_view_as_id,
            user_view_as_email,
            user_id,
            manager_id,
            email,
            full_name,
            is_superadmin,
            company_id,
            company_name,
            role_id,
            role_name,
            campaign_id,
            campaign_name,
            campaign_status,
            campaign_type
        FROM (
            (SELECT user_id user_view_as_id, * FROM user_base) 
            UNION ALL
            (SELECT manager_id user_view_as_id, * FROM user_base WHERE manager_id IS NOT NULL) 
        ) t_all
        LEFT JOIN (
            SELECT
                id,
                email user_view_as_email
            FROM
                cms_dev_silver.dim_users
        ) t_users
        ON t_all.user_view_as_id = t_users.id
        """

    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()


@asset(deps=[cms_wa_chat, cms_wa_chat_rooms, cms_users])
def dim_waba_chat(bigquery: BigQueryResource):
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_silver.dim_waba_chat"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = f"""
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
            cms_dev_bronze.wa_chats wa_chats
            LEFT JOIN cms_dev_bronze.users users
                ON wa_chats.user_id = users.id
            LEFT JOIN cms_dev_bronze.wa_chat_rooms wa_chat_rooms
                ON wa_chats.wa_chat_room_id = wa_chat_rooms.id
        WHERE
            wa_chats.is_active = 1;
        """

    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()


@asset(deps=[cms_wa_accounts])
def dim_wa_accounts(bigquery: BigQueryResource):
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_silver.dim_wa_accounts"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = f"""
        SELECT
            id,
            company_id,
            phone_number,
            active_chat_limit,
            provider_type,
            status,
            wa_phone_number_id
        FROM
            cms_dev_bronze.wa_accounts
        WHERE
            is_active = 1
        """

    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()


@asset(deps=[cms_wa_templates])
def dim_wa_templates(bigquery: BigQueryResource):
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_silver.dim_wa_templates"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = f"""
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
            cms_dev_bronze.wa_templates
        WHERE
            is_active = 1
        """

    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()


@asset(
    deps=[
        dim_user_view,
        dim_customers
    ]
)
def mv_campaign_overview(bigquery: BigQueryResource) -> None:
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_gold.mv_campaign_overview"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = """
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
            cms_dev_silver.dim_user_view
    ) user_view
    LEFT JOIN cms_dev_silver.dim_customers customers
    ON user_view.campaign_id = customers.campaign_id
    """
    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()


@asset(
    deps=[
        dim_user_view,
        dim_campaigns,
        dim_waba_chat
    ]
)
def mv_waba_chat(bigquery: BigQueryResource):
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_gold.mv_waba_chat"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = """
    SELECT
        user_campaign.user_view_as_id,
        user_campaign.user_view_as_email,
        user_campaign.user_id,
        user_campaign.email,
        user_campaign.full_name,
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
            user_view.user_id,
            user_view.email,
            user_view.full_name,
            user_view.company_id,
            user_view.company_name,
            user_view.campaign_id,
            user_view.campaign_name,
            user_view.campaign_status,
            user_view.campaign_type,
            campaigns.details_id
        FROM
            cms_dev_silver.dim_user_view user_view
                LEFT JOIN cms_dev_silver.dim_campaigns campaigns
                ON user_view.campaign_id = campaigns.id
        WHERE
            user_view.role_name = 'Agent'
    ) user_campaign 
    LEFT JOIN (
        SELECT
            wa_campaign_detail_id,
            user_id,
            COUNT(sent_timestamp) num_sent,
            SUM(CASE WHEN sent_timestamp IS NOT NULL AND delivered_timestamp IS NOT NULL THEN 1 ELSE 0 END) num_delivered,
            SUM(CASE WHEN sent_timestamp IS NOT NULL AND delivered_timestamp IS NOT NULL AND read_timestamp IS NOT NULL THEN 1 ELSE 0 END) num_read
        FROM
            cms_dev_silver.dim_waba_chat
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
                cms_dev_silver.dim_waba_chat
            GROUP BY 1
        ) t_waba1
        GROUP BY 1, 2
    ) reply_message
    ON user_message.wa_campaign_detail_id = reply_message.wa_campaign_detail_id
    AND user_message.user_id = reply_message.user_id
    """
    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()


@asset(deps=[dim_user_view, dim_wa_accounts])
def mv_wa_accounts(bigquery: BigQueryResource):
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_gold.mv_wa_accounts"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = """
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
            cms_dev_silver.dim_user_view
    ) user_view
    LEFT JOIN cms_dev_silver.dim_wa_accounts wa_accounts
    ON user_view.company_id = wa_accounts.company_id
    """
    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()
    

@asset(deps=[dim_user_view, dim_wa_templates])
def mv_wa_templates(bigquery: BigQueryResource):
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_gold.mv_wa_templates"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = """
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
            cms_dev_silver.dim_user_view
    ) user_view
    LEFT JOIN cms_dev_silver.dim_wa_templates wa_templates
    ON user_view.company_id = wa_templates.company_id
    """
    with bigquery.get_client() as client:
        job = client.query(sql, job_config=job_config)
        job.result()