-- Minimal policy model for scheduled Databricks App lifecycle management.
-- Run in the catalog/schema where the policy should live.

CREATE TABLE IF NOT EXISTS platform.app_schedule (
  workspace_id STRING NOT NULL COMMENT 'Databricks workspace ID',
  app_id       STRING NOT NULL COMMENT 'Stable Databricks App ID',
  timezone     STRING NOT NULL COMMENT 'IANA timezone, e.g. Europe/Stockholm',
  start_time   STRING NOT NULL COMMENT 'Local start time as HH:mm',
  stop_time    STRING NOT NULL COMMENT 'Local stop time as HH:mm',
  weekdays     ARRAY<INT> NOT NULL COMMENT 'ISO weekdays on which the running window starts: Monday=1 ... Sunday=7',
  app_metadata VARIANT COMMENT 'Descriptive snapshot from the Apps API; intentionally schema-free',
  updated_at   TIMESTAMP NOT NULL COMMENT 'Last owner policy update'
)
USING DELTA
COMMENT 'Owner-approved running schedules for Databricks Apps. Absence of a row means unmanaged.';

-- The UI consumes a stable relational shape while app_metadata can evolve independently.
CREATE OR REPLACE VIEW platform.app_schedule_ui AS
SELECT
  workspace_id,
  app_id,
  try_variant_get(app_metadata, '$.app_name', 'string') AS app_name,
  try_variant_get(app_metadata, '$.url', 'string') AS app_url,
  try_variant_get(app_metadata, '$.creator', 'string') AS creator,
  try_variant_get(app_metadata, '$.create_time', 'timestamp') AS create_time,
  timezone,
  start_time,
  stop_time,
  weekdays,
  updated_at
FROM platform.app_schedule;

-- Example owner preference. The UI should MERGE rows using (workspace_id, app_id).
-- No row = the automation must not touch the App.
--
-- MERGE INTO platform.app_schedule AS t
-- USING (
--   SELECT
--     '<workspace-id>' AS workspace_id,
--     '<app-id>' AS app_id,
--     'Europe/Stockholm' AS timezone,
--     '08:00' AS start_time,
--     '18:00' AS stop_time,
--     array(1,2,3,4,5) AS weekdays,
--     parse_json('{"app_name":"example","url":"https://...","creator":"owner@example.com","create_time":"2026-09-15T08:00:00Z"}') AS app_metadata,
--     current_timestamp() AS updated_at
-- ) AS s
-- ON t.workspace_id = s.workspace_id AND t.app_id = s.app_id
-- WHEN MATCHED THEN UPDATE SET *
-- WHEN NOT MATCHED THEN INSERT *;
