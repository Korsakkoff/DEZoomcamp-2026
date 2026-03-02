import marimo

__generated_with = "0.20.2"
app = marimo.App()


@app.cell
def _():
    import dlt
    import ibis
    import marimo as mo

    return dlt, ibis, mo


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
    # Compute total amount of money generated in tips
    total_tips_expr = table.aggregate(total_tips=table["tip_amt"].sum())
    total_tips_df = total_tips_expr.execute()
    total_tips_value = float(total_tips_df["total_tips"].iloc[0])

    return total_tips_value


@app.cell
def _(mo, total_tips_value):
    mo.vstack(
        [
            mo.md("# Total tip amount"),
            mo.md(
                f"**Total amount of money generated in tips:** `{total_tips_value:.2f}`"
            ),
        ]
    )
    return


if __name__ == "__main__":
    app.run()

