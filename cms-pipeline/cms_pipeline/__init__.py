from dagster import Definitions, load_assets_from_modules

from dagster_gcp import BigQueryResource

from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
        resources={
        "bigquery": BigQueryResource(
            project="telmark-gcp",  # required
            location="asia-southeast2",  # optional, defaults to the default location for the project - see https://cloud.google.com/bigquery/docs/locations for a list of locations
        )
    }
)
