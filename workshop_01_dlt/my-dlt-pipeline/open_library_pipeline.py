"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source():
    """Define dlt resources for the Open Library REST API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org/",
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "endpoint": {
                "params": {
                    # Default response format for the Books API
                    "format": "json",
                    "jscmd": "data",
                },
                "paginator": {
                    "type": "single_page",
                },
            },
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "api/books",
                    # Example book identifiers; adjust as needed
                    "params": {
                        "bibkeys": "ISBN:0451526538,ISBN:0201558025",
                    },
                    # The Books API returns a mapping keyed by bibkey,
                    # so select all top-level values as separate records.
                    "data_selector": "*",
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
