#!/usr/bin/env python
# coding: utf-8

import sys
import re
import requests
import pandas as pd
import dlt
import click
from io import BytesIO
from dlt.destinations import filesystem


url_base = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


def iter_periods(years, months):
    for year in years:
        for month in months:
            yield year, month, f"{month:02d}"


def normalize_names(df: pd.DataFrame) -> pd.DataFrame:
    def to_snake(name: str) -> str:
        name = name.strip()
        name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)  # camelCase -> snake
        name = name.replace(" ", "_")
        name = name.lower()
        return name

    df.columns = [to_snake(c) for c in df.columns]
    return df


def fix_location_id_column_names(df: pd.DataFrame) -> pd.DataFrame:
    # Algunas releases traen pulocation_id / dolocation_id
    if "pulocation_id" in df.columns and "pu_location_id" not in df.columns:
        df = df.rename(columns={"pulocation_id": "pu_location_id"})
    if "dolocation_id" in df.columns and "do_location_id" not in df.columns:
        df = df.rename(columns={"dolocation_id": "do_location_id"})
    return df


def coerce_int_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df


def coerce_datetime_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=False)
    return df


# --- Per-dataset config -------------------------------------------------------

DTYPES_BY_DATASET: dict[str, dict] = {
    # yellow/green: esquema “taxi” clásico
    "yellow": {
        "store_and_fwd_flag": "string",
        "vendor_id": "Int64",
        "passenger_count": "Int64",
        "ratecode_id": "Int64",
        "payment_type": "Int64",
        "pu_location_id": "Int64",
        "do_location_id": "Int64",
    },
    "green": {
        "store_and_fwd_flag": "string",
        "vendor_id": "Int64",
        "passenger_count": "Int64",
        "ratecode_id": "Int64",
        "payment_type": "Int64",
        "pu_location_id": "Int64",
        "do_location_id": "Int64",
        "trip_type": "Int64",
    },
    # fhv: columnas típicas del dataset FHV en el repo del zoomcamp
    "fhv": {
        "dispatching_base_num": "string",
        "affiliated_base_number": "string",
        "pu_location_id": "Int64",
        "do_location_id": "Int64",
        "sr_flag": "Int64",
    },
}

INT_COLS_BY_DATASET: dict[str, list[str]] = {
    "yellow": [
        "vendor_id",
        "passenger_count",
        "ratecode_id",
        "payment_type",
        "pu_location_id",
        "do_location_id",
    ],
    "green": [
        "vendor_id",
        "passenger_count",
        "ratecode_id",
        "payment_type",
        "pu_location_id",
        "do_location_id",
        "trip_type",
    ],
    "fhv": [
        "pu_location_id",
        "do_location_id",
        "sr_flag",
    ],
}

DATETIME_COLS_BY_DATASET: dict[str, list[str]] = {
    "yellow": ["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    "green": ["lpep_pickup_datetime", "lpep_dropoff_datetime"],
    # Ojo: en algunos ficheros aparece "dropoff_datetime" con distinta capitalización;
    # como normalizamos a snake_case, normalmente queda en:
    "fhv": ["pickup_datetime", "dropoff_datetime"],
}


def download_csv_gz_to_df(url: str, dtype: dict | None = None) -> pd.DataFrame:
    r = requests.get(url, timeout=120)
    if r.status_code == 404:
        raise FileNotFoundError(url)
    r.raise_for_status()

    # dtype puede contener columnas que NO existan en el fichero => pandas lo tolera
    return pd.read_csv(
        BytesIO(r.content),
        compression="gzip",
        dtype=dtype,
        low_memory=False,
    )


def normalize_for_dataset(df: pd.DataFrame, dataset: str) -> pd.DataFrame:
    df = normalize_names(df)
    df = fix_location_id_column_names(df)  # <-- AÑADIR ESTO

    df = coerce_int_cols(df, INT_COLS_BY_DATASET.get(dataset, []))
    df = coerce_datetime_cols(df, DATETIME_COLS_BY_DATASET.get(dataset, []))
    return df


@dlt.source(name="module4_github_rides")
def github_rides_source(datasets, years, months):
    def make_resource(dataset: str):
        @dlt.resource(name=f"{dataset}_tripdata")
        def trips():
            r_state = dlt.current.resource_state()
            processed = set(r_state.get("processed_periods", []))

            for year, month, month_str in iter_periods(years, months):
                period_key = f"{dataset}_{year}-{month_str}"
                if period_key in processed:
                    click.echo(f"[SKIP] {period_key}")
                    continue

                file_name = f"{dataset}_tripdata_{year}-{month_str}.csv.gz"
                url = f"{url_base}/{dataset}/{file_name}"

                click.echo(f"[GET ] {file_name}")
                try:
                    df = download_csv_gz_to_df(url, dtype=DTYPES_BY_DATASET.get(dataset))
                except FileNotFoundError:
                    click.echo(f"[MISS] {file_name}")
                    continue

                df["year"] = year
                df["month"] = month
                df["dataset"] = dataset

                df = normalize_for_dataset(df, dataset)

                yield df

                processed.add(period_key)
                r_state["processed_periods"] = sorted(processed)

        return trips

    return [make_resource(d) for d in datasets]


@click.command()
@click.option("--dataset", "dataset_name", default="nytaxi", help="Dataset name for dlt")
@click.option("--pipeline-name", default="module4_github_rides_pipeline", help="dlt pipeline name")
@click.option(
    "--datasets",
    multiple=True,
    default=["yellow", "green", "fhv"],
    type=click.Choice(["yellow", "green", "fhv"]),
    help="Which datasets to ingest",
)
@click.option("--years", multiple=True, default=[2019, 2020], type=int)
@click.option("--months", multiple=True, default=tuple(range(1, 13)), type=int)
def main(dataset_name, pipeline_name, datasets, years, months):
    click.echo("Starting ingestion...")
    click.echo(f"Years: {years}")
    click.echo(f"Months: {months}")
    click.echo(f"Datasets: {datasets}")

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=filesystem(layout="{schema_name}/{table_name}/{load_id}.{ext}"),
        dataset_name=dataset_name,
    )

    source = github_rides_source(datasets=list(datasets), years=list(years), months=list(months))
    load_info = pipeline.run(source, loader_file_format="csv")

    click.echo("Load finished")
    click.echo(load_info)


if __name__ == "__main__":
    sys.exit(main())