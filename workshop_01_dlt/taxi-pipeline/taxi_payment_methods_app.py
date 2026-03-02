import marimo

__generated_with = "0.20.2"
app = marimo.App()


@app.cell
def _():
    import dlt
    import ibis
    import marimo as mo
    import plotly.express as px

    return dlt, ibis, mo, px


@app.cell
def _(dlt):
    # Connect to the dlt dataset via ibis
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi_data",
    )

    dataset = pipeline.dataset()
    ibis_conn = dataset.ibis()

    # `nyc_taxi_trips` is the resource/table name created by the pipeline
    table = ibis_conn.table("nyc_taxi_trips", database=pipeline.dataset_name)
    return (table,)


@app.cell
def _(ibis, table):
    # Aggregate: proportion of trips by payment method
    # Column names come from dlt-normalized schema (lowercase with underscores)
    payment_col = "payment_type"

    trips_by_payment = (
        table.group_by(payment_col)
        .aggregate(trip_count=table.count())
        .mutate(
            proportion=lambda t: t.trip_count
            / t.trip_count.sum()
        )
        .order_by(ibis.desc("trip_count"))
    )

    df = trips_by_payment.execute()
    return df, payment_col


@app.cell
def _(df, mo, payment_col, px):
    # Simple visualization (table + bar chart)
    fig = px.bar(
        df,
        x=payment_col,
        y="proportion",
        title="Proportion of trips by payment method",
        labels={payment_col: "Payment method", "proportion": "Proportion"},
    )

    bar = mo.ui.plotly(fig)

    mo.vstack(
        [
            mo.md("# NYC taxi trips: payment methods"),
            mo.md(
                "Share of trips by `payment_type`, computed with ibis "
                "on the dlt DuckDB dataset."
            ),
            mo.hstack([bar]),
            mo.md("## Data"),
            mo.ui.table(df),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
