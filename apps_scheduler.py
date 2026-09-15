# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Apps Scheduler
# MAGIC
# MAGIC Reconciles Databricks App compute state against owner-approved schedules.
# MAGIC
# MAGIC **Safety contract**
# MAGIC - No schedule row = unmanaged App; this notebook will not touch it.
# MAGIC - A scheduled App must be restart-safe. Stopping an App releases its compute;
# MAGIC   local runtime files and in-memory state are not preserved across restart.
# MAGIC - Starting an App starts its last active deployment; this notebook does not redeploy source code.
# MAGIC
# MAGIC Adapted from Maksim Pachkouski's Databricks Apps start/stop example:
# MAGIC https://github.com/protmaks/Databricks/blob/main/Databricks%20Apps/apps_start_stop.py

# COMMAND ----------

from datetime import datetime, time
from zoneinfo import ZoneInfo
import json

from databricks.sdk import WorkspaceClient

# COMMAND ----------
# DBTITLE 1,Configuration

# Change these two values for the Unity Catalog location that stores scheduling policy.
CATALOG = "main"
SCHEMA = "platform"
SCHEDULE_TABLE_NAME = "app_schedule"


def quote_identifier(value: str) -> str:
    """Quote a Spark SQL identifier safely."""
    return f"`{value.replace('`', '``')}`"


w = WorkspaceClient()
workspace_id = str(w.get_workspace_id())

# COMMAND ----------
# DBTITLE 1,Validate configuration

catalog_names = {row["catalog"] for row in spark.sql("SHOW CATALOGS").collect()}
if CATALOG not in catalog_names:
    raise RuntimeError(
        f"Configured catalog '{CATALOG}' does not exist or is not visible to the job identity."
    )

schema_rows = spark.sql(f"SHOW SCHEMAS IN {quote_identifier(CATALOG)}").collect()
schema_names = {row["databaseName"] for row in schema_rows}
if SCHEMA not in schema_names:
    raise RuntimeError(
        f"Configured schema '{CATALOG}.{SCHEMA}' does not exist or is not visible to the job identity."
    )

SCHEDULE_TABLE = ".".join(
    [
        quote_identifier(CATALOG),
        quote_identifier(SCHEMA),
        quote_identifier(SCHEDULE_TABLE_NAME),
    ]
)

print(f"Workspace ID: {workspace_id}")
print(f"Policy location: {CATALOG}.{SCHEMA}")
print(f"Schedule table: {CATALOG}.{SCHEMA}.{SCHEDULE_TABLE_NAME}")

# COMMAND ----------
# DBTITLE 1,Load managed schedules

schedule_rows = spark.sql(
    f"""
    SELECT
      workspace_id,
      app_id,
      timezone,
      start_time,
      stop_time,
      weekdays
    FROM {SCHEDULE_TABLE}
    WHERE workspace_id = '{workspace_id}'
    """
).collect()

schedules = {row["app_id"]: row.asDict(recursive=True) for row in schedule_rows}
print(f"Managed Apps in this workspace: {len(schedules)}")

# COMMAND ----------
# DBTITLE 1,Discover Apps

def enum_value(value):
    """Return a stable string for SDK enums or plain strings."""
    if value is None:
        return "UNKNOWN"
    return getattr(value, "value", str(value))


apps = list(w.apps.list())
apps_by_id = {app.id: app for app in apps if app.id}
print(f"Discovered Apps in workspace: {len(apps_by_id)}")

# COMMAND ----------
# DBTITLE 1,Refresh descriptive metadata

metadata_rows = []
for app_id, app in apps_by_id.items():
    if app_id not in schedules:
        continue

    metadata_rows.append(
        (
            workspace_id,
            app_id,
            json.dumps(
                {
                    "app_name": app.name,
                    "url": app.url,
                    "creator": app.creator,
                    "create_time": app.create_time,
                }
            ),
        )
    )

if metadata_rows:
    metadata_df = spark.createDataFrame(
        metadata_rows,
        "workspace_id string, app_id string, metadata_json string",
    )
    metadata_df.createOrReplaceTempView("_app_metadata_refresh")

    spark.sql(
        f"""
        MERGE INTO {SCHEDULE_TABLE} AS t
        USING _app_metadata_refresh AS s
          ON t.workspace_id = s.workspace_id
         AND t.app_id = s.app_id
        WHEN MATCHED THEN UPDATE SET
          t.app_metadata = parse_json(s.metadata_json)
        """
    )

# COMMAND ----------
# DBTITLE 1,Schedule evaluation

def parse_hhmm(value: str) -> time:
    return datetime.strptime(value, "%H:%M").time()


def should_be_running(schedule: dict, now_utc: datetime) -> bool:
    """
    Evaluate the owner's desired running window.

    `weekdays` contains ISO weekday numbers (Monday=1 ... Sunday=7) for the day
    on which the running window starts. Overnight windows are supported.
    """
    tz = ZoneInfo(schedule["timezone"])
    local_now = now_utc.astimezone(tz)
    local_time = local_now.time().replace(tzinfo=None)

    start = parse_hhmm(schedule["start_time"])
    stop = parse_hhmm(schedule["stop_time"])
    weekdays = set(schedule["weekdays"] or [])
    today = local_now.isoweekday()

    if start == stop:
        # Treat identical start/stop as a 24-hour window on selected weekdays.
        return today in weekdays

    if start < stop:
        return today in weekdays and start <= local_time < stop

    # Overnight window, e.g. 20:00 -> 06:00.
    if local_time >= start:
        return today in weekdays

    previous_day = 7 if today == 1 else today - 1
    return previous_day in weekdays and local_time < stop

# COMMAND ----------
# DBTITLE 1,Reconcile desired and actual state

RUNNING_STATES = {"ACTIVE", "STARTING", "UPDATING"}
STOPPED_STATES = {"STOPPED", "STOPPING"}


def reconcile_app(app, schedule, now_utc):
    app_id = app.id
    app_name = app.name
    current_state = enum_value(app.compute_status.state if app.compute_status else None)
    desired_running = should_be_running(schedule, now_utc)

    if desired_running:
        if current_state in RUNNING_STATES:
            action = "none"
            message = f"Already {current_state}"
        else:
            w.apps.start(app_name)
            action = "start_requested"
            message = f"Requested start from {current_state}"
    else:
        if current_state in STOPPED_STATES:
            action = "none"
            message = f"Already {current_state}"
        else:
            w.apps.stop(app_name)
            action = "stop_requested"
            message = f"Requested stop from {current_state}"

    return {
        "app_id": app_id,
        "app_name": app_name,
        "state": current_state,
        "desired": "RUNNING" if desired_running else "STOPPED",
        "action": action,
        "message": message,
    }


now_utc = datetime.now(ZoneInfo("UTC"))
results = []

for app_id, schedule in schedules.items():
    app = apps_by_id.get(app_id)

    if app is None:
        result = {
            "app_id": app_id,
            "app_name": None,
            "state": "NOT_FOUND",
            "desired": None,
            "action": "failed",
            "message": "Scheduled app_id was not found in this workspace",
        }
    else:
        try:
            result = reconcile_app(app, schedule, now_utc)
        except Exception as exc:
            result = {
                "app_id": app_id,
                "app_name": app.name,
                "state": enum_value(app.compute_status.state if app.compute_status else None),
                "desired": None,
                "action": "failed",
                "message": str(exc),
            }

    results.append(result)
    print(
        f"[{result['action']}] {result['app_name'] or result['app_id']}: "
        f"{result['message']}"
    )

# COMMAND ----------
# DBTITLE 1,Summary

starts = [r for r in results if r["action"] == "start_requested"]
stops = [r for r in results if r["action"] == "stop_requested"]
unchanged = [r for r in results if r["action"] == "none"]
failed = [r for r in results if r["action"] == "failed"]

print("\nSummary")
print(f"  start requested: {len(starts)}")
print(f"  stop requested:  {len(stops)}")
print(f"  unchanged:       {len(unchanged)}")
print(f"  failed:          {len(failed)}")

if failed:
    failed_ids = ", ".join(r["app_id"] for r in failed)
    raise RuntimeError(f"Failed to reconcile {len(failed)} app(s): {failed_ids}")
