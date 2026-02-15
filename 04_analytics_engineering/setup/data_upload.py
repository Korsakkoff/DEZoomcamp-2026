#!/usr/bin/env python
# coding: utf-8

import sys 
import requests
import pandas as pd
import dlt
import click
import re
from google.cloud import storage, bigquery
from dlt.destinations import filesystem
from io import BytesIO

storage_client = storage.Client()
bucket = storage_client.bucket("dtc-de-zoomcamp-didac")
url_base = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


def iter_periods(years, months):
    for year in years:
        for month in months:
            yield year, month, f"{month:02d}"


def download_csv_gz_to_df(url: str):
    r = requests.get(url, timeout=120)
    if r.status_code == 404:
        raise FileNotFoundError(url)
    r.raise_for_status()
    return pd.read_csv(
        BytesIO(r.content),
        compression="gzip",
        dtype={
            "store_and_fwd_flag": "string", 
            "vendor_id": "Int64", 
            "passenger_count": "Int64", 
            "ratecode_id": "Int64", 
            "payment_type": "Int64",
            "pu_location_id": "Int64",
            "do_location_id": "Int64",
            "trip_type": "Int64"
        }
    )

def normalize_names(df: pd.DataFrame) -> pd.DataFrame:

    def to_snake(name: str) -> str:
        name = name.strip()
        name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
        name = name.lower()
        return name

    df.columns = [to_snake(c) for c in df.columns]
    return df


def normalize_types(df: pd.DataFrame) -> pd.DataFrame:

    INT_COLS = [
        "vendor_id",
        "passenger_count",
        "ratecode_id",
        "payment_type",
        "pu_location_id",
        "do_location_id",
        "trip_type"
    ]

    for c in INT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    return df


@dlt.source(name="module4_github_rides")
def github_rides_source(colors, years, months):

    def make_resource(color: str):

        @dlt.resource(name=f"{color}_tripdata")
        def trips():
            r_state = dlt.current.resource_state()
            processed = set(r_state.get("processed_periods", []))

            for year, month, month_str in iter_periods(years, months):
                period_key = f"{color}_{year}-{month_str}"

                if period_key in processed:
                    click.echo(f"[SKIP] {period_key}")
                    continue

                file_name = f"{color}_tripdata_{year}-{month_str}.csv.gz"
                url = f"{url_base}/{color}/{file_name}"

                click.echo(f"[GET ] {file_name}")

                try:
                    df = download_csv_gz_to_df(url)
                except FileNotFoundError:
                    click.echo(f"[MISS] {file_name}")
                    continue

                df["year"] = year
                df["month"] = month
                df["color"] = color

                df= normalize_names(df)
                df = normalize_types(df)
                yield df

                processed.add(period_key)
                r_state["processed_periods"] = sorted(processed)

        return trips

    return [make_resource(c) for c in colors]


@click.command()
#@click.option("--bucket", required=True, help="GCS bucket name")
@click.option("--dataset", default="nytaxi", help="Dataset name for dlt")
@click.option("--pipeline-name", default="module4_github_rides_pipeline", help="dlt pipeline name")
@click.option("--colors", multiple=True, default=["yellow", "green"], type=click.Choice(["yellow", "green"]))
@click.option("--years", multiple=True, default=[2019, 2020], type=int)
@click.option("--months", multiple=True, default=tuple(range(1, 13)), type=int)
#def main(bucket, dataset, pipeline_name, colors, years, months):
def main(dataset, pipeline_name, colors, years, months):

    click.echo("Starting ingestion...")
    #click.echo(f"Bucket: {bucket}")
    click.echo(f"Years: {years}")
    click.echo(f"Months: {months}")
    click.echo(f"Colors: {colors}")

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=filesystem(
            layout="{schema_name}/{table_name}/{load_id}.{ext}"
        ),
        dataset_name=dataset,
    )

    source = github_rides_source(colors=list(colors), years=list(years), months=list(months))

    load_info = pipeline.run(source, loader_file_format="csv")

    click.echo("Load finished")
    click.echo(load_info)
    click.echo(f"Months received: {months}")


if __name__ == "__main__":
    sys.exit(main())