# Databricks App Management

Minimal policy-driven scheduling for Databricks Apps across an enterprise Databricks deployment.

The goal is deliberately narrow: App owners opt in to a running schedule, and a small reconciler Job in each workspace starts or stops App compute so that actual state converges to that schedule.

## Repository contents

- `setup.sql` — creates the minimal Delta policy table and a relational UI view.
- `apps_scheduler.py` — Databricks notebook source that reconciles Apps in the current workspace.

## Safety model

Scheduling is opt-in:

- **No row in `platform.app_schedule` means unmanaged.** The automation does not touch that App.
- The owner is responsible for confirming that the App is restart-safe before creating a schedule.
- Stopping an App releases its compute. Runtime-local files and in-memory state are not preserved across restart.
- Starting an App starts its **last active deployment**. The scheduler does not redeploy from the original source location.

Databricks deployments use stable deployment artifacts, so lifecycle `start` is distinct from a new deployment. Apps that depend on state written only to their running compute must not opt in.

## Minimal data model

```text
platform.app_schedule

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

`platform.app_schedule_ui` extracts the fields needed by a simple owner-facing UI:

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
3. reads only policy rows for that workspace;
4. lists Apps through `w.apps.list()`;
5. matches Apps by `app_id`;
6. starts or stops only managed Apps;
7. refreshes descriptive `app_metadata` for those managed Apps;
8. fails the run if any managed App could not be reconciled.

All workspace Jobs can read the same Unity Catalog policy table where the catalog is shared/accessible across those workspaces.

## Setup

Run `setup.sql` in the desired catalog. The examples use schema `platform`:

```sql
CREATE SCHEMA IF NOT EXISTS platform;
```

Then import or sync `apps_scheduler.py` into each target workspace and configure it as a scheduled Databricks Job task.

The Job execution identity needs:

- `SELECT` and `MODIFY` on `platform.app_schedule` (metadata refresh uses `MERGE`);
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

Saving the form performs a `MERGE` into `platform.app_schedule`. Removing/declining scheduling deletes the row, returning the App to unmanaged status.

## Origin

The lifecycle code was originally adapted from Maksim Pachkouski's Databricks Apps start/stop example:

https://github.com/protmaks/Databricks/blob/main/Databricks%20Apps/apps_start_stop.py
