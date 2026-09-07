-- database/migrations/008_add_dataset_cancelled_status.sql
--
-- dataset_jobs already allows a 'CANCELLED' status (see 004), and the
-- orchestrator mirrors a job's terminal status onto its parent dataset
-- (DatasetManager.set_job_status), but datasets.chk_dataset_status never
-- included 'CANCELLED'. Any cancel flow that leaves the dataset row alive
-- (POST /api/datasets/{id}/cancel) needs the dataset itself to be able to
-- sit in CANCELLED status, so widen the check constraint.

ALTER TABLE datasets
    DROP CONSTRAINT IF EXISTS chk_dataset_status;

ALTER TABLE datasets
    ADD CONSTRAINT chk_dataset_status CHECK (status IN (
        'DRAFT', 'QUEUED', 'PREPARING', 'DOWNLOADING', 'PROCESSING',
        'PAUSED', 'CLEANUP', 'COMPLETED', 'FAILED', 'CANCELLED',
        'DELETING', 'DELETED'
    ));

INSERT INTO schema_migrations (version, description)
VALUES ('008', 'Allow CANCELLED status on datasets to support POST /api/datasets/{id}/cancel')
ON CONFLICT (version) DO NOTHING;
