# Databricks App Management

Minimal policy-driven scheduling for Databricks Apps across an enterprise Databricks deployment.

The goal is deliberately narrow: App owners opt in to a running schedule, and a small reconciler Job in each workspace starts or stops App compute so that actual state converges to that schedule.

## Repository contents

- `setup.sql` — creates the minimal Delta policy table and a relational UI view.
- `apps_scheduler.py` — Databricks notebook source that reconciles Apps in the current workspace.

## Scheduler configuration

At the top of `apps_scheduler.py`, configure the Unity Catalog location used for scheduling policy:

```python
CATALOG = "main"
SCHEMA = "platform"
SCHEDULE_TABLE_NAME = "app_schedule"
```

The scheduler validates `CATALOG` and `SCHEMA` before it reads policy or changes any App state. If either does not exist, or is not visible to the Job execution identity, the notebook raises an exception and stops immediately.

The scheduler never creates catalogs or schemas. Provision them separately, then point every workspace Job at the intended shared policy location.

## Safety model

Scheduling is opt-in:

- **No row in the configured `app_schedule` table means unmanaged.** The automation does not touch that App.
- The owner is responsible for confirming that the App is restart-safe before creating a schedule.
- Stopping an App releases its compute. Runtime-local files and in-memory state are not preserved across restart.
- Starting an App starts its **last active deployment**. The scheduler does not redeploy from the original source location.

Databricks deployments use stable deployment artifacts, so lifecycle `start` is distinct from a new deployment. Apps that depend on state written only to their running compute must not opt in.

## Minimal data model

```text
<CATALOG>.<SCHEMA>.app_schedule

workspace_id   STRING       stable workspace identity
app_id         STRING       stable App identity
timezone       STRING       IANA timezone, e.g. Europe/Stockholm
start_time     STRING       HH:mm local time
stop_time      STRING       HH:mm local time
weekdays       ARRAY<INT>   ISO weekdays: Monday=1 ... Sunday=7
app_metadata   VARIANT      descriptive API snapshot
updated_at     TIMESTAMP    last owner policy update
```

`workspace_id + app_id` is the logical key.

The schedule fields are intentionally typed and small. Descriptive fields such as App name, URL, creator and creation time live in `app_metadata`, so new metadata can be added without changing the table schema.

### UI view

`<CATALOG>.<SCHEMA>.app_schedule_ui` extracts the fields needed by a simple owner-facing UI:

- workspace ID
- App ID
- App name
- App URL
- creator
- creation time
- timezone
- start/stop times
- weekdays
- last policy update

The UI should write policy rows using `(workspace_id, app_id)` as the key. It should not use App name as identity.

## Why `app_id` and not App name?

The policy is keyed by `app_id`. The Apps lifecycle API addresses an App by its current name, so the reconciler discovers Apps in the workspace, matches the schedule using `app_id`, and then uses the API-reported current name for the start/stop call.

This means an App rename does not invalidate its schedule.

## Schedule semantics

The row describes the time during which the App should be running.

Example:

```text
timezone    Europe/Stockholm
start_time  08:00
stop_time   18:00
weekdays    [1,2,3,4,5]
```

means:

```text
Mon-Fri 08:00-18:00  -> desired state RUNNING
all other times      -> desired state STOPPED
```

Overnight windows such as `20:00 -> 06:00` are supported. In that case `weekdays` refers to the day on which the running window starts.

## Reconciliation instead of clock-triggered commands

Do not create separate "start at 08:00" and "stop at 18:00" jobs.

Run the same notebook periodically, for example every 30 or 60 minutes:

```text
current App state + owner schedule
              |
              v
      calculate desired state
              |
       +------+------+
       |             |
    RUNNING       STOPPED
       |             |
   ensure start    ensure stop
```

This is idempotent and self-healing. If an 18:00 run fails, the next run still sees that the desired state is `STOPPED` and retries the reconciliation.

## Enterprise deployment

Deploy the same Job to each workspace rather than centrally authenticating into every workspace.

Each Job:

1. creates a notebook-native `WorkspaceClient()`;
2. obtains its own workspace ID with `w.get_workspace_id()`;
3. validates the configured catalog and schema;
4. reads only policy rows for that workspace;
5. lists Apps through `w.apps.list()`;
6. matches Apps by `app_id`;
7. starts or stops only managed Apps;
8. refreshes descriptive `app_metadata` for those managed Apps;
9. fails the run if any managed App could not be reconciled.

All workspace Jobs can read the same Unity Catalog policy table where the configured catalog is shared/accessible across those workspaces.

## Setup

Create the target catalog/schema separately, then run `setup.sql` in that location. The SQL example currently uses schema `platform`; adjust it to match the values configured in `apps_scheduler.py`.

Then import or sync `apps_scheduler.py` into each target workspace and configure it as a scheduled Databricks Job task.

The Job execution identity needs:

- visibility/use permissions on the configured catalog and schema;
- `SELECT` and `MODIFY` on the schedule table (metadata refresh uses `MERGE`);
- permission to list the target Apps;
- permission to start and stop the managed Apps.

## Owner UI contract

A minimal form needs only:

```text
App: <display name / URL>
Timezone: <IANA timezone>
Running days: Mon Tue Wed Thu Fri
Start: 08:00
Stop: 18:00

[ ] I confirm this App is safe to restart and does not rely on
    runtime-local files or in-memory state surviving a stop/start.

Save
```

Saving the form performs a `MERGE` into the configured schedule table. Removing/declining scheduling deletes the row, returning the App to unmanaged status.

## Origin

The lifecycle code was originally adapted from Maksim Pachkouski's Databricks Apps start/stop example:

https://github.com/protmaks/Databricks/blob/main/Databricks%20Apps/apps_start_stop.py
