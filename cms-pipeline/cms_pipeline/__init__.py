from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from dagster_gcp import BigQueryResource

from . import cms_assets, shopee_assets

all_assets = load_assets_from_modules([cms_assets, shopee_assets])

cms_job = define_asset_job("cms_job", selection=AssetSelection.all())

cms_schedule = ScheduleDefinition(
    job=cms_job,
    cron_schedule="0 7 * * *",  # every day at 7am UTC
)

defs = Definitions(
    assets=all_assets,
    resources={
        "bigquery": BigQueryResource(
            project="telmark-gcp",  
            location="asia-southeast2",
        )
    },
    schedules=[cms_schedule]
)
