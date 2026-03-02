import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_pipeline() -> dlt.sources.DltSource:
    """
    REST API source for NYC taxi data.

    The API returns paginated JSON with 1,000 records per page.
    Pagination is implemented via a page-number parameter and stops
    automatically once the API starts returning empty pages.
    """

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
            # Page-number based pagination; dlt will stop when an empty page is returned.
            "paginator": {
                "type": "page_number",
                "page_param": "page",
                # dlt expects `base_page` for the starting page number.
                "base_page": 1,
                # API does not return a total pages field, so disable it
                # and bound pagination by a large maximum_page while relying
                # on stop_after_empty_page (defaults to True).
                "total_path": None,
                "maximum_page": 1_000_000,
            },
        },
        "resource_defaults": {
            "endpoint": {
                # The API returns 1,000 records per page.
                "params": {
                    "page_size": 1000,
                },
            },
        },
        "resources": [
            {
                "name": "nyc_taxi_trips",
                # Empty path means we call the base URL directly.
                "endpoint": {
                    "path": "",
                },
            }
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nyc_taxi_data",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline())
    print(load_info)  # noqa: T201

