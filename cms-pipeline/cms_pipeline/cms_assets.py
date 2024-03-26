from dagster import asset

from dagster_gcp import BigQueryResource

from .cms_utils import cms_to_bronze, execute_sql_file



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
            "name",
            "is_agent"
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
def cms_campaign_flow_customers(bigquery: BigQueryResource) -> None:
    cms_to_bronze(
        bigquery=bigquery,
        source_table="campaign_flow_customers",
        dest_table="campaign_flow_customers",
        add_cols=[
            "campaign_flow_id",
            "customer_id",
            "user_id",
            "status",
            "expiration_time",
            "total_unread",
            "last_interact_time"
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
    execute_sql_file(bigquery, "silver", "dim_users")


@asset(deps=[cms_users_roles, cms_roles, cms_companies_users_managers])
def dim_roles(bigquery: BigQueryResource) -> None:
    execute_sql_file(bigquery, "silver", "dim_roles")


@asset(deps=[cms_companies])
def dim_companies(bigquery: BigQueryResource) -> None:
    execute_sql_file(bigquery, "silver", "dim_companies")


@asset(deps=[cms_campaigns, cms_wa_campaign_details])
def dim_campaigns(bigquery: BigQueryResource) -> None:
    execute_sql_file(bigquery, "silver", "dim_campaigns")


@asset(deps=[cms_customers, cms_campaign_customers])
def dim_customers(bigquery: BigQueryResource):
    execute_sql_file(bigquery, "silver", "dim_customers")


@asset(deps=[dim_users, dim_roles, dim_campaigns])
def dim_user_view(bigquery: BigQueryResource) -> None:
    execute_sql_file(bigquery, "silver", "dim_user_view")


@asset(deps=[cms_wa_chat, cms_wa_chat_rooms, cms_users])
def dim_waba_chat(bigquery: BigQueryResource):
    execute_sql_file(bigquery, "silver", "dim_waba_chat")


@asset(deps=[cms_wa_accounts])
def dim_wa_accounts(bigquery: BigQueryResource):
    execute_sql_file(bigquery, "silver", "dim_wa_accounts")


@asset(deps=[cms_wa_templates])
def dim_wa_templates(bigquery: BigQueryResource):
    execute_sql_file(bigquery, "silver", "dim_wa_templates")


@asset(deps=[cms_campaign_flow_customers])
def fact_agent_assignments(bigquery: BigQueryResource) -> None:
    execute_sql_file(bigquery, "silver", "fact_agent_assignments")


@asset(deps=[dim_user_view, dim_customers, fact_agent_assignments])
def mv_campaign_overview(bigquery: BigQueryResource) -> None:
    execute_sql_file(bigquery, "gold", "mv_campaign_overview")


@asset(deps=[dim_user_view, dim_campaigns, dim_waba_chat])
def mv_waba_chat(bigquery: BigQueryResource):
    execute_sql_file(bigquery, "gold", "mv_waba_chat")


@asset(deps=[dim_user_view, dim_wa_accounts])
def mv_wa_accounts(bigquery: BigQueryResource):
    execute_sql_file(bigquery, "gold", "mv_wa_accounts")
    

@asset(deps=[dim_user_view, dim_wa_templates])
def mv_wa_templates(bigquery: BigQueryResource):
    execute_sql_file(bigquery, "gold", "mv_wa_templates")