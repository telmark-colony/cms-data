from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from dagster_gcp import BigQueryResource

from . import cms_assets, cms_real_time_assets, shopee_assets

# CMS HOURLY

cms_asset_list = load_assets_from_modules([cms_assets], group_name="cms_assets")

cms_job = define_asset_job("cms_job", selection=AssetSelection.groups("cms_assets"))

cms_schedule = ScheduleDefinition(
    job=cms_job,
    cron_schedule="0 * * * *", # hourly
)

# CMS MINUTELY

cms_real_time_asset_list = load_assets_from_modules([cms_real_time_assets], group_name="cms_real_time_assets")

cms_real_time_job = define_asset_job("cms_real_time_job", selection=AssetSelection.groups("cms_real_time_assets"))

cms_real_time_schedule = ScheduleDefinition(
    job=cms_real_time_job,
    cron_schedule="*/5 * * * *", # 5 minutely
)

# SHOPEE COLLECTION DAILY

shopee_asset_list = load_assets_from_modules([shopee_assets], group_name="shopee_assets")

shopee_job = define_asset_job("shopee_job", selection=AssetSelection.groups("shopee_assets"))

shopee_schedule = ScheduleDefinition(
    job=shopee_job,
    cron_schedule="0 7 * * *",  # every day at 7am UTC
)

defs = Definitions(
    assets=[*cms_asset_list, *cms_real_time_asset_list, *shopee_asset_list],
    resources={
        "bigquery": BigQueryResource(
            project="telmark-gcp",  
            location="asia-southeast2",
        )
    },
    schedules=[cms_schedule, cms_real_time_schedule, shopee_schedule]
)
