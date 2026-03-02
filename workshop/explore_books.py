import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import dlt
    import altair as alt

    return alt, dlt, mo


@app.cell
def _(dlt):
    pipeline = dlt.pipeline(
        pipeline_name="open_library_pipeline",
        destination="duckdb",
    )
    dataset = pipeline.dataset()
    return (dataset,)


@app.cell
def _(mo):
    mo.md("""
    # Open Library: Harry Potter Books Explorer
    """)
    return


@app.cell
def _(alt, dataset, mo):
    _author_df = dataset("""
        SELECT value AS author, COUNT(DISTINCT _dlt_parent_id) AS book_count
        FROM books__author_name
        GROUP BY value
        ORDER BY book_count DESC
        LIMIT 20
    """).df()

    _bar_chart = (
        alt.Chart(_author_df)
        .mark_bar()
        .encode(
            x=alt.X("book_count:Q", title="Number of Books"),
            y=alt.Y("author:N", sort="-x", title="Author"),
            tooltip=["author", "book_count"],
        )
        .properties(title="Top 20 Authors by Number of Books", width=600, height=400)
    )

    mo.vstack([mo.md("## Books per Author"), _bar_chart])
    return


@app.cell
def _(alt, dataset, mo):
    _timeline_df = dataset("""
        SELECT first_publish_year AS year, COUNT(*) AS book_count
        FROM books
        WHERE first_publish_year >= 1900 AND first_publish_year <= 2025
        GROUP BY first_publish_year
        ORDER BY first_publish_year
    """).df()

    _line_chart = (
        alt.Chart(_timeline_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("year:Q", title="Year", axis=alt.Axis(format="d")),
            y=alt.Y("book_count:Q", title="Number of Books"),
            tooltip=["year", "book_count"],
        )
        .properties(title="Harry Potter Books Published Over Time", width=600, height=400)
    )

    mo.vstack([mo.md("## Books Over Time"), _line_chart])
    return


if __name__ == "__main__":
    app.run()
