# Databricks notebook source
# MAGIC %md
# MAGIC # Shutdown Databricks Apps
# MAGIC
# MAGIC Stops one Databricks App or all Apps in the workspace.
# MAGIC
# MAGIC Intended to be run manually or as a scheduled Databricks Job to reduce
# MAGIC unnecessary App compute outside working hours.
# MAGIC
# MAGIC Adapted from Maksim Pachkouski's Databricks Apps start/stop example:
# MAGIC https://github.com/protmaks/Databricks/blob/main/Databricks%20Apps/apps_start_stop.py

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------
# DBTITLE 1,Select target

def list_apps():
    response = w.api_client.do("GET", "/api/2.0/apps")
    return response.get("apps", [])


apps = list_apps()
app_names = ["all"] + [app["name"] for app in apps]

dbutils.widgets.dropdown("app_name", "all", app_names)
app_name = dbutils.widgets.get("app_name")

print(f"Target: {app_name}")

# COMMAND ----------
# DBTITLE 1,Shutdown

def stop_app(name):
    """Stop an App unless it is already stopping or stopped."""
    app_info = w.api_client.do("GET", f"/api/2.0/apps/{name}")
    current_state = app_info.get("compute_status", {}).get("state", "UNKNOWN")

    if current_state in ("STOPPING", "STOPPED"):
        return {
            "app": name,
            "previous_state": current_state,
            "result": "skipped",
            "message": f"Already {current_state}",
        }

    w.api_client.do("POST", f"/api/2.0/apps/{name}/stop")
    return {
        "app": name,
        "previous_state": current_state,
        "result": "stop_requested",
        "message": "Stop request submitted",
    }


if app_name.lower() == "all":
    targets = [app["name"] for app in list_apps()]
else:
    targets = [app_name]

print(f"Stopping {len(targets)} app(s)...")

results = []
for name in targets:
    try:
        result = stop_app(name)
    except Exception as exc:
        result = {
            "app": name,
            "previous_state": "UNKNOWN",
            "result": "failed",
            "message": str(exc),
        }

    results.append(result)
    print(f"[{result['result']}] {name}: {result['message']}")

# COMMAND ----------
# DBTITLE 1,Summary

failed = [result for result in results if result["result"] == "failed"]
requested = [result for result in results if result["result"] == "stop_requested"]
skipped = [result for result in results if result["result"] == "skipped"]

print("\nSummary")
print(f"  stop requested: {len(requested)}")
print(f"  skipped:        {len(skipped)}")
print(f"  failed:         {len(failed)}")

if failed:
    failed_names = ", ".join(result["app"] for result in failed)
    raise RuntimeError(f"Failed to stop {len(failed)} app(s): {failed_names}")
