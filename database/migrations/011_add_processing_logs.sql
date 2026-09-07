-- database/migrations/011_add_processing_logs.sql
--
-- Structured, append-only pipeline log. Previously the only "log" available
-- was scene_job_state (mutable, current-stage-only, no duration/size/quality
-- fields) surfaced via GET /api/datasets/{id}/logs. This table gives every
-- stage transition (STARTED/RUNNING/COMPLETED/FAILED) its own row with a
-- free-form JSONB details blob (duration_seconds, file_size_mb,
-- quality_score, memory_peak_mb, cpu_peak_percent, error_type,
-- error_message, traceback, progress_percent, ...).

CREATE TABLE IF NOT EXISTS processing_logs (
    log_id       BIGSERIAL     PRIMARY KEY,
    log_uuid     UUID          NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
    dataset_id   INTEGER       NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    scene_id     VARCHAR(255)  NOT NULL,
    module       VARCHAR(50)   NOT NULL,
    stage        VARCHAR(50)   NOT NULL,
    status       VARCHAR(20)   NOT NULL,
    message      TEXT          NOT NULL,
    details      JSONB         NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processing_logs_dataset_created ON processing_logs (dataset_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_processing_logs_dataset_scene   ON processing_logs (dataset_id, scene_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_processing_logs_stage_status    ON processing_logs (stage, status);

COMMENT ON TABLE processing_logs IS 'Structured append-only pipeline log: one row per stage STARTED/RUNNING/COMPLETED/FAILED event.';

INSERT INTO schema_migrations (version, description)
VALUES ('011', 'Add processing_logs: structured append-only pipeline log (stage/duration/size/quality/memory)')
ON CONFLICT (version) DO NOTHING;
