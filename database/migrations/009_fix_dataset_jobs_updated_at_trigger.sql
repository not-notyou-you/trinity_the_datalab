-- database/migrations/009_fix_dataset_jobs_updated_at_trigger.sql
--
-- Migration 004 attached trg_dataset_jobs_updated_at (fn_set_updated_at,
-- which does `NEW.updated_at = NOW()`) to dataset_jobs, but dataset_jobs
-- was never given an updated_at column. Every UPDATE on dataset_jobs
-- (pause, resume, cancel, delete, and the orchestrator's own status
-- transitions) has therefore been failing with:
--   psycopg2.errors.UndefinedColumn: record "new" has no field "updated_at"
--
-- dataset_jobs doesn't need an updated_at column (nothing reads it), so
-- just drop the trigger instead of adding the column.

DROP TRIGGER IF EXISTS trg_dataset_jobs_updated_at ON dataset_jobs;

INSERT INTO schema_migrations (version, description)
VALUES ('009', 'Drop trg_dataset_jobs_updated_at: dataset_jobs has no updated_at column, so every UPDATE on it was failing')
ON CONFLICT (version) DO NOTHING;
