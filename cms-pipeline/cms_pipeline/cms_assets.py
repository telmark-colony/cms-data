from typing import List

from dagster import asset

from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq


DEFAULT_COLUMNS = [
    "id", 
    "is_active", 
    "created_at",
    "created_by", 
    "updated_at",
    "updated_by", 
    "deleted_at",
    "deleted_by"
]


def cms_to_bronze(
        bigquery: BigQueryResource,
        source_table: str = None,
        dest_table: str = None,
        add_cols: List[str] = []
    ) -> None:
    if source_table and dest_table:
        job_config = bq.QueryJobConfig(
            destination=f"telmark-gcp.cms_dev_bronze.{dest_table}"
        )
        job_config.write_disposition = "WRITE_TRUNCATE"
        columns = DEFAULT_COLUMNS + add_cols
        sql = f"""
            SELECT * FROM
                EXTERNAL_QUERY(
                    'telmark-gcp.asia-southeast2.cms-db-development', 
                    'SELECT {",".join(columns)} FROM {source_table};'
                )
            """

        with bigquery.get_client() as client:
            job = client.query(sql, job_config=job_config)
            job.result()


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
            wa_campaign_details.provider_type wa_provider_type
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
        dim_users,
        dim_companies,
        dim_roles,
        dim_campaigns
    ]
)
def mv_users(bigquery: BigQueryResource) -> None:
    job_config = bq.QueryJobConfig(
        destination="telmark-gcp.cms_dev_gold.mv_users"
    )
    job_config.write_disposition = "WRITE_TRUNCATE"
    sql = f"""
        WITH user_base AS (
            SELECT
                users.id as user_id,
                roles.manager_id,
                users.email,
                users.is_superadmin,
                companies.name as company_name,
                roles.role_name,
                campaigns.name as campaign_name,
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
            is_superadmin,
            company_name,
            role_name,
            campaign_name,
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