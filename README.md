# Databricks App Management

A minimal Databricks notebook for shutting down Databricks Apps when they are not needed.

The primary use case is cost control: run the notebook from a scheduled Databricks Job outside working hours to stop App compute that would otherwise remain active.

## Repository contents

- `apps_shutdown.py` — Databricks notebook source that stops one App or all Apps in the current workspace.

## How it works

The notebook authenticates with `WorkspaceClient()` using the identity of the Databricks Job or interactive user running it. It then uses the Databricks Apps REST API to:

1. List Apps in the workspace.
2. Check each selected App's current compute state.
3. Skip Apps that are already `STOPPING` or `STOPPED`.
4. Submit a stop request for the remaining Apps.
5. Continue processing other Apps if one fails, then fail the run at the end if any shutdown request failed.

App lifecycle changes are asynchronous. A successful result means the stop request was accepted; it does not mean the App has already reached `STOPPED` when the notebook finishes.

## Usage

Import or sync `apps_shutdown.py` into a Databricks workspace and run it on compute with the Databricks SDK available.

The notebook exposes an `app_name` widget:

- `all` — stop every App visible to the execution identity.
- `<app name>` — stop only that App.

For scheduled cost control, leave the widget at `all` and configure the notebook as a Databricks Job task.

## Scheduling example

A simple setup is to schedule the Job for the end of the working week, for example Friday at 17:00 in the desired timezone.

This repository currently handles shutdown only. Restart scheduling, exception lists, maintenance windows, and policy-based reconciliation are intentionally out of scope for this minimal version.

## Permissions

The execution identity must be able to list the target Databricks Apps and stop them. The exact permissions depend on how Apps are owned and governed in the workspace.

## Origin

Adapted from Maksim Pachkouski's Databricks Apps start/stop notebook:

https://github.com/protmaks/Databricks/blob/main/Databricks%20Apps/apps_start_stop.py
