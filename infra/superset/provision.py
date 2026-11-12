"""Provision StreamLoom once; preserve subsequent edits made in Superset."""
import json
import os
import re
from urllib.parse import quote

from superset import db, security_manager
from superset.app import create_app


def metric(column, aggregate):
    return {
        "expressionType": "SIMPLE", "column": {"column_name": column},
        "aggregate": aggregate, "label": column, "optionName": f"{aggregate}_{column}",
    }


def provision():
    from superset.connectors.sqla.models import SqlaTable, TableColumn
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    host = os.environ["CLICKHOUSE_HOST"]
    if not host or host == "your-instance.clickhouse.cloud":
        raise ValueError("Renseigner CLICKHOUSE_HOST dans .env avant make superset-setup")
    schema = os.environ.get("CLICKHOUSE_DATABASE", "default")
    # Restrict the identifier interpolated in dataset SQL; credentials are URL-encoded.
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("CLICKHOUSE_DATABASE doit être un identifiant SQL simple")
    owner = security_manager.find_user(username=os.environ.get("SUPERSET_ADMIN_USERNAME", "admin"))
    database = db.session.query(Database).filter_by(database_name="StreamLoom ClickHouse").one_or_none()
    if database is None:
        user = quote(os.environ.get("CLICKHOUSE_USER", "default"), safe="")
        password = quote(os.environ["CLICKHOUSE_PASSWORD"], safe="")
        port = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
        database = Database(database_name="StreamLoom ClickHouse", expose_in_sqllab=True)
        database.set_sqlalchemy_uri(f"clickhousedb://{user}:{password}@{host}:{port}/{schema}?secure=true")
        db.session.add(database)
        db.session.flush()

    datasets = {}
    for period, table in (("minute", "eod_agg_1m"), ("heure", "eod_agg_1h"), ("jour", "eod_agg_daily")):
        name = f"streamloom_{period}"
        dataset = db.session.query(SqlaTable).filter_by(database_id=database.id, table_name=name).one_or_none()
        if dataset is None:
            dataset = SqlaTable(
                table_name=name, database=database, owners=[owner],
                main_dttm_col="time", sql=f"""SELECT
  parseDateTimeBestEffort(toString(window_start), 'UTC') AS time,
  symbol, open, high, low, close, volume_sum,
  100.0 * (close - open) / nullIf(open, 0) AS pct_change
FROM `{schema}`.{table}""",
            )
            dataset.columns = [
                TableColumn(column_name=name, type=kind, is_dttm=(name == "time"),
                            groupby=True, filterable=True)
                for name, kind in (
                    ("time", "DateTime"), ("symbol", "String"),
                    ("open", "Float64"), ("high", "Float64"), ("low", "Float64"),
                    ("close", "Float64"), ("volume_sum", "UInt64"), ("pct_change", "Float64"),
                )
            ]
            db.session.add(dataset)
            db.session.flush()
        datasets[period] = dataset

    # Latest available day, with explicitly ordered opening/closing values.
    dataset = db.session.query(SqlaTable).filter_by(database_id=database.id, table_name="streamloom_derniere_seance").one_or_none()
    if dataset is None:
        dataset = SqlaTable(
            table_name="streamloom_derniere_seance", database=database, owners=[owner],
            sql=f"""WITH quotes AS (
  SELECT parseDateTimeBestEffort(toString(window_start), 'UTC') AS time,
         symbol, open, close, volume_sum FROM `{schema}`.eod_agg_1m
)
SELECT symbol, sum(volume_sum) AS total_volume,
       100.0 * (argMax(close, time) - argMin(open, time))
       / nullIf(argMin(open, time), 0) AS pct_change
FROM quotes
WHERE toDate(time) = (SELECT max(toDate(time)) FROM quotes)
GROUP BY symbol""",
        )
        dataset.columns = [TableColumn(column_name=name, type=kind, groupby=True, filterable=True)
                           for name, kind in (("symbol", "String"), ("total_volume", "UInt64"), ("pct_change", "Float64"))]
        db.session.add(dataset)
        db.session.flush()
    datasets["seance"] = dataset

    charts = []
    definitions = [
        ("Clôture par minute", "minute", "echarts_timeseries_line", "close", "MAX", "PT1M"),
        ("Volumes par minute", "minute", "echarts_timeseries_bar", "volume_sum", "SUM", "PT1M"),
        ("Clôture par heure", "heure", "echarts_timeseries_line", "close", "MAX", "PT1H"),
        ("Volumes par heure", "heure", "echarts_timeseries_bar", "volume_sum", "SUM", "PT1H"),
        ("Clôture par jour", "jour", "echarts_timeseries_line", "close", "MAX", "P1D"),
        ("Volumes par jour", "jour", "echarts_timeseries_bar", "volume_sum", "SUM", "P1D"),
        ("Top volumes — dernière séance", "seance", "table", "total_volume", "MAX", None),
        ("Top variations — dernière séance", "seance", "table", "pct_change", "MAX", None),
    ]
    for title, period, viz, column, aggregate, grain in definitions:
        title = f"StreamLoom — {title}"
        chart = db.session.query(Slice).filter_by(slice_name=title).one_or_none()
        if chart is None:
            source = datasets[period]
            measure = metric(column, aggregate)
            params = {
                "datasource": f"{source.id}__table", "viz_type": viz,
                "metrics": [measure], "groupby": ["symbol"], "adhoc_filters": [],
                "row_limit": 10 if viz == "table" else 10000,
                "time_range": "No filter" if viz == "table" else "Last 7 days",
            }
            if viz == "table":
                params.update(query_mode="aggregate", order_by_cols=[],
                              order_desc=True, timeseries_limit_metric=measure,
                              order_by_metric=True, percent_metrics=[])
            else:
                params.update(x_axis="time", granularity_sqla="time", time_grain_sqla=grain,
                              show_legend=True, rich_tooltip=True, truncate_metric=True)
            chart = Slice(slice_name=title, datasource_id=source.id, datasource_type="table",
                          viz_type=viz, params=json.dumps(params), owners=[owner], cache_timeout=0)
            db.session.add(chart)
            db.session.flush()
        charts.append(chart)

    dashboard = db.session.query(Dashboard).filter_by(slug="streamloom").one_or_none()
    if dashboard is None:
        layout = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
            "GRID_ID": {"id": "GRID_ID", "type": "GRID", "children": [], "parents": ["ROOT_ID"]},
            "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": "StreamLoom"}},
        }
        dashboard = Dashboard(
            dashboard_title="StreamLoom — Cotations boursières", slug="streamloom",
            published=True, owners=[owner], slices=[], position_json=json.dumps(layout),
            json_metadata=json.dumps({"refresh_frequency": 30, "timed_refresh_immune_slices": []}),
        )
        db.session.add(dashboard)
    else:
        layout = json.loads(dashboard.position_json)

    # Extend an existing dashboard without replacing its layout, filters or charts.
    existing_ids = {chart.id for chart in dashboard.slices}
    positioned_ids = {item.get("meta", {}).get("chartId") for item in layout.values() if isinstance(item, dict)}
    missing = [chart for chart in charts if chart.id not in positioned_ids]
    for i in range(0, len(missing), 2):
        row_id = f"ROW-STREAMLOOM-{missing[i].id}"
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {"id": row_id, "type": "ROW", "children": [],
                          "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for chart in missing[i:i + 2]:
            chart_id = f"CHART-{chart.id}"
            layout[row_id]["children"].append(chart_id)
            layout[chart_id] = {"id": chart_id, "type": "CHART", "children": [],
                               "parents": ["ROOT_ID", "GRID_ID", row_id],
                               "meta": {"chartId": chart.id, "width": 6, "height": 50, "sliceName": chart.slice_name}}
    for chart in charts:
        if chart.id not in existing_ids:
            dashboard.slices.append(chart)
    dashboard.position_json = json.dumps(layout)
    db.session.commit()
    print("Dashboard prêt : http://localhost:8088/superset/dashboard/streamloom/")


if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        provision()
