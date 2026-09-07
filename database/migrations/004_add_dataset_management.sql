-- database/migrations/004_add_dataset_management.sql

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id           SERIAL          PRIMARY KEY,
    dataset_uuid         UUID            NOT NULL UNIQUE DEFAULT uuid_generate_v4(),
    name                 VARCHAR(255)    NOT NULL,
    description          TEXT,
    location_label       VARCHAR(255),
    region_id            INTEGER         REFERENCES regions_of_interest(region_id) ON DELETE SET NULL,
    bbox                 GEOMETRY(POLYGON, 4326) NOT NULL,
    bbox_wkt             TEXT            NOT NULL,
    date_start           DATE            NOT NULL,
    date_end             DATE            NOT NULL,
    required_tiers       TEXT[]          NOT NULL DEFAULT ARRAY['GOLD'],
    quality_settings     JSONB           NOT NULL DEFAULT '{}',
    dataset_kind         VARCHAR(10)     NOT NULL DEFAULT 'STANDARD',
    status               VARCHAR(20)     NOT NULL DEFAULT 'DRAFT',
    total_scenes         INTEGER         NOT NULL DEFAULT 0,
    completed_scenes     INTEGER         NOT NULL DEFAULT 0,
    failed_scenes        INTEGER         NOT NULL DEFAULT 0,
    total_size_bytes     BIGINT          NOT NULL DEFAULT 0,
    is_deletable         BOOLEAN         NOT NULL DEFAULT TRUE,
    -- Ditambahkan oleh migrasi 016; disamakan di sini supaya instalasi baru
    -- tidak perlu langsung menambal kolom yang sama.
    generate_preview     BOOLEAN         NOT NULL DEFAULT TRUE,
    live_enabled         BOOLEAN         NOT NULL DEFAULT FALSE,
    live_last_checked_at TIMESTAMPTZ,
    created_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    deleted_at           TIMESTAMPTZ,
    CONSTRAINT chk_dataset_kind CHECK (dataset_kind IN ('STANDARD', 'LIVE')),
    CONSTRAINT chk_dataset_status CHECK (status IN (
        'DRAFT', 'QUEUED', 'PREPARING', 'DOWNLOADING', 'PROCESSING',
        'PAUSED', 'CLEANUP', 'COMPLETED', 'FAILED', 'DELETING', 'DELETED'
    )),
    -- FUSION ditambahkan oleh migrasi 014 (tier FUSION lahir di migrasi 013).
    -- Whitelist di sini disamakan supaya instalasi baru tidak perlu langsung
    -- menambal constraint yang sama lewat 014.
    CONSTRAINT chk_required_tiers CHECK (
        required_tiers <@ ARRAY['RAW', 'BRONZE', 'SILVER', 'GOLD', 'FUSION']::TEXT[]
        AND array_length(required_tiers, 1) > 0
    ),
    CONSTRAINT chk_dataset_date_range CHECK (date_end >= date_start)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_single_live_dataset
    ON datasets (dataset_kind) WHERE dataset_kind = 'LIVE';
CREATE INDEX IF NOT EXISTS idx_datasets_status
    ON datasets (status) WHERE status <> 'DELETED';
CREATE INDEX IF NOT EXISTS idx_datasets_kind
    ON datasets (dataset_kind);
CREATE INDEX IF NOT EXISTS idx_datasets_bbox
    ON datasets USING GIST (bbox);
CREATE INDEX IF NOT EXISTS idx_datasets_created_at
    ON datasets (created_at DESC);

CREATE TABLE IF NOT EXISTS dataset_jobs (
    job_id           BIGSERIAL       PRIMARY KEY,
    job_uuid         UUID            NOT NULL UNIQUE DEFAULT uuid_generate_v4(),
    dataset_id       INTEGER         NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    job_type         VARCHAR(20)     NOT NULL DEFAULT 'CREATE',
    status           VARCHAR(20)     NOT NULL DEFAULT 'QUEUED',
    paused_at        TIMESTAMPTZ,
    paused_by        VARCHAR(20),
    pause_reason     TEXT,
    resumed_at       TIMESTAMPTZ,
    resume_count     SMALLINT        NOT NULL DEFAULT 0,
    date_range_start DATE,
    date_range_end   DATE,
    total_scenes     INTEGER         NOT NULL DEFAULT 0,
    downloaded_count INTEGER         NOT NULL DEFAULT 0,
    processed_count  INTEGER         NOT NULL DEFAULT 0,
    failed_count     INTEGER         NOT NULL DEFAULT 0,
    cleaned_count    INTEGER         NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    CONSTRAINT chk_dataset_job_type CHECK (job_type IN ('CREATE', 'BACKFILL', 'LIVE_INGEST')),
    CONSTRAINT chk_dataset_job_status CHECK (status IN (
        'QUEUED', 'PREPARING', 'DOWNLOADING', 'PROCESSING',
        'PAUSED', 'CLEANUP', 'COMPLETED', 'FAILED', 'CANCELLED'
    ))
);

CREATE INDEX IF NOT EXISTS idx_dataset_jobs_dataset
    ON dataset_jobs (dataset_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dataset_jobs_status
    ON dataset_jobs (status);

CREATE TABLE IF NOT EXISTS scene_job_state (
    id                  BIGSERIAL       PRIMARY KEY,
    job_id              BIGINT          NOT NULL REFERENCES dataset_jobs(job_id) ON DELETE CASCADE,
    product_identifier  VARCHAR(200)    NOT NULL,
    scene_id            INTEGER         REFERENCES satellite_scenes(scene_id) ON DELETE SET NULL,
    current_stage       VARCHAR(30),
    stage_status        VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
    produced_files      JSONB           NOT NULL DEFAULT '{}',
    attempt_number      SMALLINT        NOT NULL DEFAULT 1,
    max_retries         SMALLINT        NOT NULL DEFAULT 3,
    last_error          TEXT,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    CONSTRAINT uq_job_product UNIQUE (job_id, product_identifier),
    CONSTRAINT chk_scene_job_stage_status CHECK (stage_status IN (
        'PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'SKIPPED'
    ))
);

CREATE INDEX IF NOT EXISTS idx_scene_job_state_job
    ON scene_job_state (job_id);
CREATE INDEX IF NOT EXISTS idx_scene_job_state_stage_status
    ON scene_job_state (stage_status);
CREATE INDEX IF NOT EXISTS idx_scene_job_state_scene_id
    ON scene_job_state (scene_id) WHERE scene_id IS NOT NULL;

-- dataset_id is intentionally NOT a FK to datasets: a cleanup_operations row
-- must survive its dataset being deleted so deletion-progress can still be
-- read after the delete completes (see migration 006).
CREATE TABLE IF NOT EXISTS cleanup_operations (
    id             BIGSERIAL       PRIMARY KEY,
    dataset_id     INTEGER         NOT NULL,
    job_id         BIGINT          REFERENCES dataset_jobs(job_id) ON DELETE SET NULL,
    operation_type VARCHAR(20)     NOT NULL,
    status         VARCHAR(20)     NOT NULL DEFAULT 'PENDING',
    total_files    INTEGER         NOT NULL DEFAULT 0,
    deleted_count  INTEGER         NOT NULL DEFAULT 0,
    freed_bytes    BIGINT          NOT NULL DEFAULT 0,
    error_log      TEXT,
    created_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    started_at     TIMESTAMPTZ,
    completed_at   TIMESTAMPTZ,
    CONSTRAINT chk_cleanup_op_type CHECK (operation_type IN ('TIER_CLEANUP', 'FULL_DELETE')),
    CONSTRAINT chk_cleanup_op_status CHECK (status IN ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_cleanup_ops_dataset
    ON cleanup_operations (dataset_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cleanup_ops_status
    ON cleanup_operations (status);

CREATE TABLE IF NOT EXISTS live_dataset_sources (
    id             SERIAL          PRIMARY KEY,
    source_name    VARCHAR(20)     NOT NULL UNIQUE,
    enabled        BOOLEAN         NOT NULL DEFAULT TRUE,
    last_check     TIMESTAMPTZ,
    last_ingest    TIMESTAMPTZ,
    next_check     TIMESTAMPTZ,
    source_config  JSONB           NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_live_source_name CHECK (source_name IN ('SENTINEL1', 'MODIS', 'GPM'))
);

CREATE INDEX IF NOT EXISTS idx_live_sources_enabled
    ON live_dataset_sources (enabled) WHERE enabled = TRUE;

ALTER TABLE data_products
    ADD COLUMN IF NOT EXISTS dataset_id INTEGER REFERENCES datasets(dataset_id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_dprods_dataset_id
    ON data_products (dataset_id, product_tier);

DO $$
DECLARE
    tbl TEXT;
    tbls TEXT[] := ARRAY['datasets', 'dataset_jobs', 'live_dataset_sources'];
BEGIN
    FOREACH tbl IN ARRAY tbls LOOP
        EXECUTE format('
            DROP TRIGGER IF EXISTS trg_%I_updated_at ON %I;
            CREATE TRIGGER trg_%I_updated_at
            BEFORE UPDATE ON %I
            FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
        ', tbl, tbl, tbl, tbl);
    END LOOP;
END $$;

INSERT INTO live_dataset_sources (source_name, enabled, source_config)
VALUES
    ('SENTINEL1', TRUE,  '{"mission": "S1A/S1B", "mode": "IW", "product_type": "GRD"}'),
    ('MODIS',     FALSE, '{"products": ["MCDWD"], "tiles": ["h30v08", "h31v08"]}'),
    ('GPM',       FALSE, '{"products": ["IMERGDF"]}')
ON CONFLICT (source_name) DO NOTHING;

INSERT INTO datasets (
    name, description, location_label, region_id, bbox, bbox_wkt,
    date_start, date_end, required_tiers, dataset_kind, status,
    is_deletable, live_enabled
)
SELECT
    'Dataset Live',
    'Dataset live 24/7 untuk pemantauan berkelanjutan, hanya menyimpan tier GOLD',
    'Jabodetabek',
    r.region_id,
    r.bbox,
    'POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))',
    CURRENT_DATE,
    CURRENT_DATE,
    ARRAY['GOLD'],
    'LIVE',
    'DRAFT',
    FALSE,
    FALSE
FROM regions_of_interest r
WHERE r.region_code = 'JABODTK'
ON CONFLICT (dataset_kind) WHERE dataset_kind = 'LIVE' DO NOTHING;

INSERT INTO schema_migrations (version, description)
VALUES ('004', 'Dataset management: datasets, dataset_jobs, scene_job_state, cleanup_operations, live_dataset_sources, data_products.dataset_id')
ON CONFLICT (version) DO NOTHING;