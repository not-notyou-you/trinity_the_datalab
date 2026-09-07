
-- ---------------------------------------------------------------------------
-- EXTENSIONS
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- CUSTOM TYPES (ENUMS)
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE orbit_direction_enum   AS ENUM ('ASCENDING', 'DESCENDING');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE job_status_enum        AS ENUM ('QUEUED', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE product_tier_enum      AS ENUM ('RAW', 'BRONZE', 'SILVER', 'GOLD', 'FUSION');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE storage_location_enum  AS ENUM ('LOCAL', 'S3', 'GCS', 'AZURE_BLOB');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE rule_type_enum         AS ENUM ('THRESHOLD', 'TRANSFORMATION', 'VALIDATION', 'FILTER');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE alert_severity_enum    AS ENUM ('INFO', 'WARNING', 'CRITICAL');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE alert_event_type_enum  AS ENUM ('DATA_ARRIVAL', 'QUALITY_WARNING', 'PIPELINE_ERROR', 'THRESHOLD_BREACH', 'SYSTEM_ALERT');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE http_method_enum       AS ENUM ('GET', 'POST', 'PUT', 'PATCH', 'DELETE');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- =============================================================================
-- TABLE 1: REGIONS_OF_INTEREST
-- Purpose : Master AOI (Area of Interest) definitions. Defined first because
--           Satellite_Scenes references it.
-- 3NF     : All non-key attributes depend solely on region_id (no transitive deps)
-- =============================================================================
CREATE TABLE IF NOT EXISTS regions_of_interest (
    region_id          SERIAL          PRIMARY KEY,
    region_code        VARCHAR(20)     NOT NULL UNIQUE,        -- e.g. 'JKT', 'JABODTK'
    name               VARCHAR(100)    NOT NULL,               -- e.g. 'Jabodetabek'
    description        TEXT,
    bbox               GEOMETRY(POLYGON, 4326) NOT NULL,       -- WGS84 bounding polygon
    centroid           GEOMETRY(POINT,   4326),                -- auto-computed centroid
    area_km2           NUMERIC(12,4),                          -- area in square km
    admin_level        SMALLINT        NOT NULL DEFAULT 2,     -- 1=national, 2=province, 3=city
    country_code       CHAR(2)         NOT NULL DEFAULT 'ID',
    is_active          BOOLEAN         NOT NULL DEFAULT TRUE,
    source             VARCHAR(20)     NOT NULL DEFAULT 'USER',  -- SEEDER | USER | GEOCODE
    deleted_at         TIMESTAMPTZ,                            -- NULL = aktif (soft-delete)
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_roi_source CHECK (source IN ('SEEDER', 'USER', 'GEOCODE'))
);

-- Indexes: region_code lookup, spatial query, active filter, name search
CREATE INDEX IF NOT EXISTS idx_roi_region_code  ON regions_of_interest (region_code);
CREATE INDEX IF NOT EXISTS idx_roi_bbox         ON regions_of_interest USING GIST (bbox);
CREATE INDEX IF NOT EXISTS idx_roi_is_active    ON regions_of_interest (is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_roi_admin_level  ON regions_of_interest (admin_level);
CREATE INDEX IF NOT EXISTS idx_roi_name_lower   ON regions_of_interest (lower(name));
CREATE INDEX IF NOT EXISTS idx_roi_source       ON regions_of_interest (source);
CREATE INDEX IF NOT EXISTS idx_roi_not_deleted  ON regions_of_interest (deleted_at) WHERE deleted_at IS NULL;

COMMENT ON TABLE  regions_of_interest IS 'Master table of geographic Areas of Interest (AOI) for Sentinel-1 acquisition coverage.';
COMMENT ON COLUMN regions_of_interest.bbox IS 'WGS84 polygon bounding box, stored as PostGIS geometry for spatial queries.';
COMMENT ON COLUMN regions_of_interest.admin_level IS '1=national, 2=province/region, 3=city/district level granularity.';
COMMENT ON COLUMN regions_of_interest.source IS 'SEEDER = bawaan sistem (tidak bisa dihapus), USER = ditambahkan lewat UI, GEOCODE = auto-dibuat location_resolver dari Nominatim.';
COMMENT ON COLUMN regions_of_interest.deleted_at IS 'Waktu soft-delete. NULL = aktif. Baris tidak pernah dihapus fisik karena satellite_scenes/datasets mereferensikan region_id (ON DELETE RESTRICT).';

-- =============================================================================
-- TABLE 2: PROCESSING_STAGES
-- Purpose : Defines each ETL pipeline stage (ordered metadata). Allows dynamic
--           stage configuration without code changes.
-- 3NF     : stage_name → timeout_minutes, retry_count (no partial/transitive deps)
-- =============================================================================
CREATE TABLE IF NOT EXISTS processing_stages (
    stage_id           SERIAL          PRIMARY KEY,
    stage_name         VARCHAR(50)     NOT NULL UNIQUE,        -- e.g. 'DOWNLOAD', 'LEE_FILTER'
    stage_code         VARCHAR(20)     NOT NULL UNIQUE,        -- short code e.g. 'DL', 'LF'
    stage_order        SMALLINT        NOT NULL,               -- execution sequence
    description        TEXT,
    timeout_minutes    SMALLINT        NOT NULL DEFAULT 60,
    retry_count        SMALLINT        NOT NULL DEFAULT 3,
    retry_delay_sec    SMALLINT        NOT NULL DEFAULT 30,
    is_mandatory       BOOLEAN         NOT NULL DEFAULT TRUE,
    is_active          BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_stage_order UNIQUE (stage_order)
);

CREATE INDEX IF NOT EXISTS idx_pstages_order       ON processing_stages (stage_order);
CREATE INDEX IF NOT EXISTS idx_pstages_is_active   ON processing_stages (is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_pstages_stage_name  ON processing_stages (stage_name);

COMMENT ON TABLE  processing_stages IS 'Ordered ETL pipeline stage definitions. Controls execution sequence and retry policy.';
COMMENT ON COLUMN processing_stages.stage_order IS 'Strict sequential order: 1=Download, 2=Crop, 3=Lee Filter, 4=COG Export, 5=Orchestrate, 6=Analytics.';

-- =============================================================================
-- TABLE 3: SATELLITE_SCENES
-- Purpose : Core registry of all Sentinel-1 SAR scenes discovered/downloaded.
--           Root entity - almost all other tables FK back to this.
-- 3NF     : scene_id → all attributes. polarizations extracted to avoid multivalued.
--           orbit_pass, acquisition fields all functionally depend on scene_id only.
-- =============================================================================
CREATE TABLE IF NOT EXISTS satellite_scenes (
    scene_id               SERIAL              PRIMARY KEY,
    scene_uuid             UUID                NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
    product_identifier     VARCHAR(200)        NOT NULL UNIQUE,   -- ESA product ID
    platform               VARCHAR(20)         NOT NULL DEFAULT 'SENTINEL-1',
    instrument_mode        VARCHAR(10)         NOT NULL DEFAULT 'IW',  -- IW, EW, SM, WV
    polarization_vv        BOOLEAN             NOT NULL DEFAULT TRUE,
    polarization_vh        BOOLEAN             NOT NULL DEFAULT TRUE,
    acquisition_datetime   TIMESTAMPTZ         NOT NULL,          -- UTC acquisition time
    orbit_number           INTEGER,
    orbit_direction        orbit_direction_enum NOT NULL DEFAULT 'ASCENDING',
    relative_orbit         SMALLINT,
    bbox                   GEOMETRY(POLYGON, 4326) NOT NULL,       -- scene footprint
    cloud_cover_percent    NUMERIC(5,2)        CHECK (cloud_cover_percent BETWEEN 0 AND 100),
    incidence_angle_near   NUMERIC(6,3),                          -- degrees
    incidence_angle_far    NUMERIC(6,3),
    resolution_m           SMALLINT            NOT NULL DEFAULT 10,
    region_id              INTEGER             NOT NULL REFERENCES regions_of_interest(region_id) ON DELETE RESTRICT,
    raw_file_path          TEXT,                                   -- path to original download
    raw_file_size_mb       NUMERIC(12,3),
    download_url           TEXT,
    checksum_md5           VARCHAR(32),
    is_available           BOOLEAN             NOT NULL DEFAULT TRUE,
    created_at             TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

-- Time-series index (acquisition time is primary query axis)
CREATE INDEX IF NOT EXISTS idx_scenes_acq_dt       ON satellite_scenes (acquisition_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_scenes_bbox          ON satellite_scenes USING GIST (bbox);
CREATE INDEX IF NOT EXISTS idx_scenes_region_id     ON satellite_scenes (region_id);
CREATE INDEX IF NOT EXISTS idx_scenes_orbit_dir     ON satellite_scenes (orbit_direction);
CREATE INDEX IF NOT EXISTS idx_scenes_product_id    ON satellite_scenes (product_identifier);
CREATE INDEX IF NOT EXISTS idx_scenes_available     ON satellite_scenes (is_available, acquisition_datetime DESC) WHERE is_available = TRUE;

COMMENT ON TABLE  satellite_scenes IS 'Core registry of all Sentinel-1 SAR scenes. Root entity for the entire pipeline.';
COMMENT ON COLUMN satellite_scenes.product_identifier IS 'ESA Copernicus Hub product identifier (unique globally).';
COMMENT ON COLUMN satellite_scenes.acquisition_datetime IS 'UTC timestamp of SAR acquisition pass. Primary time-series axis.';

-- Convert to TimescaleDB hypertable (partition by acquisition_datetime, 1-month chunks)
-- NOTE: Run only if TimescaleDB extension is installed; safe to skip for plain PostgreSQL.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'satellite_scenes',
            'acquisition_datetime',
            chunk_time_interval => INTERVAL '1 month',
            if_not_exists => TRUE
        );
        RAISE NOTICE 'TimescaleDB hypertable created for satellite_scenes';
    ELSE
        RAISE NOTICE 'TimescaleDB not installed - satellite_scenes remains plain PostgreSQL table';
    END IF;
END $$;

-- =============================================================================
-- TABLE 4: PROCESSING_JOBS
-- Purpose : Execution tracking for each ETL stage run per scene. One row per
--           (scene × stage) execution attempt.
-- 3NF     : job_id → scene_id, stage_id, status, times. No transitive deps.
-- =============================================================================
CREATE TABLE IF NOT EXISTS processing_jobs (
    job_id             BIGSERIAL       PRIMARY KEY,
    job_uuid           UUID            NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
    scene_id           INTEGER         NOT NULL REFERENCES satellite_scenes(scene_id) ON DELETE CASCADE,
    stage_id           INTEGER         NOT NULL REFERENCES processing_stages(stage_id) ON DELETE RESTRICT,
    attempt_number     SMALLINT        NOT NULL DEFAULT 1,
    status             job_status_enum NOT NULL DEFAULT 'QUEUED',
    queued_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    started_at         TIMESTAMPTZ,
    completed_at       TIMESTAMPTZ,
    duration_seconds   NUMERIC(10,3)   GENERATED ALWAYS AS (
                           EXTRACT(EPOCH FROM (completed_at - started_at))
                       ) STORED,
    worker_hostname    VARCHAR(100),
    cpu_usage_percent  NUMERIC(5,2),
    memory_usage_mb    NUMERIC(10,2),
    input_size_mb      NUMERIC(12,3),
    output_size_mb     NUMERIC(12,3),
    error_code         VARCHAR(50),
    error_message      TEXT,
    log_file_path      TEXT,
    parameters_json    JSONB           NOT NULL DEFAULT '{}',
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_job_scene_stage_attempt UNIQUE (scene_id, stage_id, attempt_number)
);

CREATE INDEX IF NOT EXISTS idx_pjobs_scene_id    ON processing_jobs (scene_id);
CREATE INDEX IF NOT EXISTS idx_pjobs_stage_id    ON processing_jobs (stage_id);
CREATE INDEX IF NOT EXISTS idx_pjobs_status      ON processing_jobs (status, queued_at DESC);
CREATE INDEX IF NOT EXISTS idx_pjobs_queued_at   ON processing_jobs (queued_at DESC);
CREATE INDEX IF NOT EXISTS idx_pjobs_failed      ON processing_jobs (status, scene_id) WHERE status = 'FAILED';

COMMENT ON TABLE  processing_jobs IS 'ETL job execution log. One row per scene × stage × attempt. Enables retry tracking and performance analysis.';
COMMENT ON COLUMN processing_jobs.duration_seconds IS 'Auto-computed from completed_at - started_at via generated column.';

-- =============================================================================
-- TABLE 5: DATA_PRODUCTS
-- Purpose : Output artifact registry. Each pipeline stage produces one product
--           per scene (raw TIFF, cropped TIFF, filtered TIFF, COG).
-- 3NF     : product_id → all attrs. product_tier kept here (not scene) because
--           tier describes the product, not the scene.
-- =============================================================================
CREATE TABLE IF NOT EXISTS data_products (
    product_id         BIGSERIAL           PRIMARY KEY,
    product_uuid       UUID                NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
    scene_id           INTEGER             NOT NULL REFERENCES satellite_scenes(scene_id) ON DELETE CASCADE,
    job_id             BIGINT              NOT NULL REFERENCES processing_jobs(job_id) ON DELETE RESTRICT,
    product_tier       product_tier_enum   NOT NULL,              -- RAW/BRONZE/SILVER/GOLD/FUSION
    source             VARCHAR(20)         NOT NULL DEFAULT 'SENTINEL1',  -- SENTINEL1 | MODIS | GPM | FUSION
    product_type       VARCHAR(50)         NOT NULL,              -- 'CROPPED_TIFF', 'LEE_FILTERED', 'COG', etc.
    band_name          VARCHAR(10)         NOT NULL,              -- 'VV', 'VH', 'NDVI', 'RAIN_24H'
    file_name          VARCHAR(255)        NOT NULL,
    file_path          TEXT                NOT NULL,
    file_size_mb       NUMERIC(12,3)       NOT NULL,
    file_format        VARCHAR(20)         NOT NULL DEFAULT 'TIFF',  -- TIFF, COG, NetCDF
    data_hash_sha256   VARCHAR(64)         NOT NULL,              -- integrity check
    crs                VARCHAR(50)         NOT NULL DEFAULT 'EPSG:4326',
    pixel_size_m       NUMERIC(8,3),
    nodata_value       NUMERIC,
    rows               INTEGER,
    cols               INTEGER,
    band_count         SMALLINT            NOT NULL DEFAULT 1,
    storage_location   storage_location_enum NOT NULL DEFAULT 'LOCAL',
    is_valid           BOOLEAN             NOT NULL DEFAULT TRUE,
    is_latest          BOOLEAN             NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_dprods_source CHECK (source IN ('SENTINEL1', 'MODIS', 'GPM', 'FUSION'))
);

CREATE INDEX IF NOT EXISTS idx_dprods_scene_id    ON data_products (scene_id);
CREATE INDEX IF NOT EXISTS idx_dprods_job_id      ON data_products (job_id);
CREATE INDEX IF NOT EXISTS idx_dprods_tier        ON data_products (product_tier, scene_id);
CREATE INDEX IF NOT EXISTS idx_dprods_hash        ON data_products (data_hash_sha256);
CREATE INDEX IF NOT EXISTS idx_dprods_latest      ON data_products (is_latest, product_tier) WHERE is_latest = TRUE;
CREATE INDEX IF NOT EXISTS idx_dprods_created_at  ON data_products (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dprods_tier_source ON data_products (product_tier, source);
CREATE INDEX IF NOT EXISTS idx_dprods_dataset_tier_source ON data_products (dataset_id, product_tier, source) WHERE is_latest = TRUE;

COMMENT ON TABLE  data_products IS 'Output artifact registry. Tracks every file produced by each ETL stage (COG, filtered TIFF, etc.).';
COMMENT ON COLUMN data_products.data_hash_sha256 IS 'SHA-256 hash of file content. Used for deduplication and integrity validation.';
COMMENT ON COLUMN data_products.product_tier IS 'Lakehouse tier: RAW (original), BRONZE (cropped ke AOI), SILVER (processed per-source), GOLD (analysis-ready per-source COG), FUSION (HDF5 multi-modal gabungan).';
COMMENT ON COLUMN data_products.source IS 'Sensor asal produk: SENTINEL1 | MODIS | GPM, atau FUSION untuk stack gabungan. Sama dengan level {source} di path on-disk (etl/folder_manager.py).';

-- =============================================================================
-- TABLE 6: QUALITY_METRICS
-- Purpose : Per-scene, per-band quality validation results. Decoupled from
--           Data_Products to allow quality recomputation without re-processing.
-- 3NF     : metric_id → scene_id, band. quality_score depends only on metric_id.
-- =============================================================================
CREATE TABLE IF NOT EXISTS quality_metrics (
    metric_id                  BIGSERIAL       PRIMARY KEY,
    scene_id                   INTEGER         NOT NULL REFERENCES satellite_scenes(scene_id) ON DELETE CASCADE,
    product_id                 BIGINT          NOT NULL REFERENCES data_products(product_id) ON DELETE CASCADE,
    band_name                  VARCHAR(10)     NOT NULL,
    assessed_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    -- Pixel-level stats
    total_pixels               BIGINT          NOT NULL,
    valid_pixels               BIGINT          NOT NULL,
    nodata_pixels              BIGINT          NOT NULL DEFAULT 0,
    nodata_percent             NUMERIC(5,2)    GENERATED ALWAYS AS (
                                   CASE WHEN total_pixels > 0
                                        THEN ROUND((nodata_pixels::NUMERIC / total_pixels) * 100, 2)
                                        ELSE 0 END
                               ) STORED,
    -- Radiometric stats
    backscatter_mean_db        NUMERIC(8,4),
    backscatter_std_db         NUMERIC(8,4),
    backscatter_min_db         NUMERIC(8,4),
    backscatter_max_db         NUMERIC(8,4),
    -- Quality thresholds
    cloud_threshold_percent    NUMERIC(5,2)    NOT NULL DEFAULT 20.0,
    radiometric_consistency    BOOLEAN,
    speckle_index              NUMERIC(8,4),   -- lower = better (Lee filter effectiveness)
    -- Composite score (0-100)
    quality_score              NUMERIC(5,2)    NOT NULL CHECK (quality_score BETWEEN 0 AND 100),
    quality_flag               VARCHAR(20)     NOT NULL DEFAULT 'UNCHECKED',  -- PASS/FAIL/WARNING/UNCHECKED
    notes                      TEXT,
    created_at                 TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_quality_scene_product_band UNIQUE (scene_id, product_id, band_name)
);

CREATE INDEX IF NOT EXISTS idx_qmetrics_scene_id      ON quality_metrics (scene_id);
CREATE INDEX IF NOT EXISTS idx_qmetrics_product_id    ON quality_metrics (product_id);
CREATE INDEX IF NOT EXISTS idx_qmetrics_quality_flag  ON quality_metrics (quality_flag, assessed_at DESC);
CREATE INDEX IF NOT EXISTS idx_qmetrics_score         ON quality_metrics (quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_qmetrics_assessed_at   ON quality_metrics (assessed_at DESC);

COMMENT ON TABLE  quality_metrics IS 'Per-scene per-band radiometric and quality validation results.';
COMMENT ON COLUMN quality_metrics.quality_score IS 'Composite quality score 0-100. Aggregates nodata%, radiometric consistency, speckle index.';
COMMENT ON COLUMN quality_metrics.speckle_index IS 'SAR speckle quality index (Coefficient of Variation). Lower = better spatial quality.';

-- =============================================================================
-- TABLE 7: PROCESSING_RULES
-- Purpose : Configurable validation/threshold rules per pipeline stage.
--           Decoupled from code — rule changes don't require redeployment.
-- 3NF     : rule_id → stage_id, rule_type, threshold. No transitive deps.
-- =============================================================================
CREATE TABLE IF NOT EXISTS processing_rules (
    rule_id            SERIAL          PRIMARY KEY,
    stage_id           INTEGER         NOT NULL REFERENCES processing_stages(stage_id) ON DELETE CASCADE,
    rule_name          VARCHAR(100)    NOT NULL,
    rule_code          VARCHAR(30)     NOT NULL UNIQUE,
    rule_type          rule_type_enum  NOT NULL,
    description        TEXT,
    threshold_value    NUMERIC(12,4),
    threshold_unit     VARCHAR(20),                               -- '%', 'dB', 'pixels', etc.
    operator           VARCHAR(10),                               -- '>', '<', '>=', '<=', '=='
    action_on_fail     VARCHAR(50)     NOT NULL DEFAULT 'WARN',  -- 'WARN', 'FAIL', 'SKIP'
    is_mandatory       BOOLEAN         NOT NULL DEFAULT TRUE,
    is_active          BOOLEAN         NOT NULL DEFAULT TRUE,
    version            VARCHAR(20)     NOT NULL DEFAULT '1.0.0',
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_prules_stage_id    ON processing_rules (stage_id);
CREATE INDEX IF NOT EXISTS idx_prules_rule_code   ON processing_rules (rule_code);
CREATE INDEX IF NOT EXISTS idx_prules_is_active   ON processing_rules (is_active, stage_id) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_prules_mandatory   ON processing_rules (is_mandatory) WHERE is_mandatory = TRUE;

COMMENT ON TABLE  processing_rules IS 'Configurable ETL validation rules per stage. Threshold-driven quality gates without code changes.';
COMMENT ON COLUMN processing_rules.action_on_fail IS 'WARN=log only, FAIL=abort job, SKIP=skip scene from pipeline.';

-- =============================================================================
-- TABLE 8: DATA_LINEAGE
-- Purpose : Parent-child transformation graph. Enables full provenance tracing
--           from raw download to final COG product.
-- 3NF     : lineage_id → parent_product_id, child_product_id, transformation_type.
--           transformation_type is atomic (not decomposed further here).
-- =============================================================================
CREATE TABLE IF NOT EXISTS data_lineage (
    lineage_id             BIGSERIAL       PRIMARY KEY,
    parent_product_id      BIGINT          NOT NULL REFERENCES data_products(product_id) ON DELETE CASCADE,
    child_product_id       BIGINT          NOT NULL REFERENCES data_products(product_id) ON DELETE CASCADE,
    transformation_type    VARCHAR(50)     NOT NULL,              -- 'CROP', 'LEE_FILTER', 'COG_EXPORT', etc.
    stage_id               INTEGER         NOT NULL REFERENCES processing_stages(stage_id) ON DELETE RESTRICT,
    job_id                 BIGINT          NOT NULL REFERENCES processing_jobs(job_id) ON DELETE RESTRICT,
    transformation_params  JSONB           NOT NULL DEFAULT '{}', -- bbox, filter_params, etc.
    input_checksum         VARCHAR(64),                           -- parent SHA-256 at time of transform
    output_checksum        VARCHAR(64),                           -- child SHA-256 after transform
    created_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_lineage_parent_child UNIQUE (parent_product_id, child_product_id),
    CONSTRAINT chk_lineage_no_self_ref CHECK (parent_product_id <> child_product_id)
);

CREATE INDEX IF NOT EXISTS idx_lineage_parent_id    ON data_lineage (parent_product_id);
CREATE INDEX IF NOT EXISTS idx_lineage_child_id     ON data_lineage (child_product_id);
CREATE INDEX IF NOT EXISTS idx_lineage_transform     ON data_lineage (transformation_type);
CREATE INDEX IF NOT EXISTS idx_lineage_job_id        ON data_lineage (job_id);
CREATE INDEX IF NOT EXISTS idx_lineage_stage_id      ON data_lineage (stage_id);

COMMENT ON TABLE  data_lineage IS 'Directed acyclic graph (DAG) of product transformations. Full provenance from RAW to GOLD tier.';
COMMENT ON COLUMN data_lineage.transformation_params IS 'JSONB of ETL parameters used: crop bbox, lee filter window size, COG compression settings, etc.';

-- =============================================================================
-- TABLE 9: DATASET_VERSIONS
-- Purpose : Semantic versioning of production-ready datasets. Decouples internal
--           product_id from external release versioning.
-- 3NF     : version_id → product_id, version_number, release_date. No transitive deps.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dataset_versions (
    version_id         SERIAL          PRIMARY KEY,
    version_uuid       UUID            NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
    product_id         BIGINT          NOT NULL REFERENCES data_products(product_id) ON DELETE RESTRICT,
    version_number     VARCHAR(20)     NOT NULL,                  -- SemVer: '1.0.0', '1.1.0'
    release_date       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    release_notes      TEXT,
    change_log         TEXT,
    is_production      BOOLEAN         NOT NULL DEFAULT FALSE,
    is_deprecated      BOOLEAN         NOT NULL DEFAULT FALSE,
    deprecated_at      TIMESTAMPTZ,
    deprecated_reason  TEXT,
    released_by        VARCHAR(100),
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_version_product_semver UNIQUE (product_id, version_number)
);

CREATE INDEX IF NOT EXISTS idx_dversions_product_id     ON dataset_versions (product_id);
CREATE INDEX IF NOT EXISTS idx_dversions_is_production  ON dataset_versions (is_production, release_date DESC) WHERE is_production = TRUE;
CREATE INDEX IF NOT EXISTS idx_dversions_version_num    ON dataset_versions (version_number);
CREATE INDEX IF NOT EXISTS idx_dversions_release_date   ON dataset_versions (release_date DESC);

COMMENT ON TABLE  dataset_versions IS 'Semantic versioning (SemVer) for production dataset releases. Supports rollback and deprecation.';

-- =============================================================================
-- TABLE 10: API_ACCESS_LOGS
-- Purpose : Audit trail for all API requests. TimescaleDB hypertable candidate
--           (high-volume append-only time-series).
-- 3NF     : log_id → all cols. endpoint and ip are independent attributes.
-- =============================================================================
CREATE TABLE IF NOT EXISTS api_access_logs (
    log_id             BIGSERIAL       PRIMARY KEY,
    log_uuid           UUID            NOT NULL DEFAULT uuid_generate_v4(),
    endpoint           VARCHAR(200)    NOT NULL,
    http_method        http_method_enum NOT NULL DEFAULT 'GET',
    user_ip            INET            NOT NULL,
    user_agent         TEXT,
    request_timestamp  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    scene_id_queried   INTEGER         REFERENCES satellite_scenes(scene_id) ON DELETE SET NULL,
    product_id_queried BIGINT          REFERENCES data_products(product_id) ON DELETE SET NULL,
    query_params       JSONB           DEFAULT '{}',
    response_status    SMALLINT        NOT NULL,                  -- HTTP status code
    response_time_ms   INTEGER         NOT NULL,
    response_size_kb   NUMERIC(12,3),
    error_detail       TEXT,
    api_key_id         VARCHAR(50),                               -- for future auth
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_apilogs_timestamp    ON api_access_logs (request_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_apilogs_endpoint     ON api_access_logs (endpoint, request_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_apilogs_status       ON api_access_logs (response_status);
CREATE INDEX IF NOT EXISTS idx_apilogs_scene_id     ON api_access_logs (scene_id_queried) WHERE scene_id_queried IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_apilogs_user_ip      ON api_access_logs (user_ip);

COMMENT ON TABLE  api_access_logs IS 'Full API audit trail. High-volume append-only table - TimescaleDB hypertable recommended.';

-- Convert API_Access_Logs to TimescaleDB hypertable (7-day chunks - high volume)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'api_access_logs',
            'request_timestamp',
            chunk_time_interval => INTERVAL '7 days',
            if_not_exists => TRUE
        );
        RAISE NOTICE 'TimescaleDB hypertable created for api_access_logs';
    ELSE
        RAISE NOTICE 'TimescaleDB not installed - api_access_logs remains plain table';
    END IF;
END $$;

-- =============================================================================
-- TABLE 11: ALERT_EVENTS
-- Purpose : Pipeline monitoring events and quality warning notifications.
--           TimescaleDB hypertable for time-series alert queries.
-- 3NF     : alert_id → event_type, severity, scene_id. All non-key attrs
--           depend solely on alert_id.
-- =============================================================================
CREATE TABLE IF NOT EXISTS alert_events (
    alert_id           BIGSERIAL           PRIMARY KEY,
    alert_uuid         UUID                NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
    event_type         alert_event_type_enum NOT NULL,
    severity           alert_severity_enum NOT NULL DEFAULT 'INFO',
    scene_id           INTEGER             REFERENCES satellite_scenes(scene_id) ON DELETE SET NULL,
    job_id             BIGINT              REFERENCES processing_jobs(job_id) ON DELETE SET NULL,
    product_id         BIGINT              REFERENCES data_products(product_id) ON DELETE SET NULL,
    title              VARCHAR(200)        NOT NULL,
    message            TEXT                NOT NULL,
    metadata_json      JSONB               DEFAULT '{}',          -- flexible context data
    is_resolved        BOOLEAN             NOT NULL DEFAULT FALSE,
    resolved_at        TIMESTAMPTZ,
    resolved_by        VARCHAR(100),
    resolution_note    TEXT,
    triggered_at       TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    created_at         TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_triggered_at  ON alert_events (triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_severity      ON alert_events (severity, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_event_type    ON alert_events (event_type);
CREATE INDEX IF NOT EXISTS idx_alerts_scene_id      ON alert_events (scene_id) WHERE scene_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_alerts_unresolved    ON alert_events (is_resolved, severity) WHERE is_resolved = FALSE;

COMMENT ON TABLE  alert_events IS 'Pipeline monitoring and quality alert events. Supports real-time operational monitoring.';
COMMENT ON COLUMN alert_events.metadata_json IS 'Flexible JSONB for event-specific context: thresholds breached, affected files, error traces.';

-- Convert Alert_Events to TimescaleDB hypertable (1-month chunks)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable(
            'alert_events',
            'triggered_at',
            chunk_time_interval => INTERVAL '1 month',
            if_not_exists => TRUE
        );
        RAISE NOTICE 'TimescaleDB hypertable created for alert_events';
    ELSE
        RAISE NOTICE 'TimescaleDB not installed - alert_events remains plain table';
    END IF;
END $$;

-- =============================================================================
-- TRIGGERS: auto-update updated_at timestamps
-- =============================================================================
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    tbl TEXT;
    tbls TEXT[] := ARRAY[
        'regions_of_interest',
        'processing_stages',
        'satellite_scenes',
        'processing_jobs',
        'data_products',
        'processing_rules',
        'dataset_versions'
    ];
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

-- Trigger: auto-compute centroid for ROI
CREATE OR REPLACE FUNCTION fn_compute_roi_centroid()
RETURNS TRIGGER AS $$
BEGIN
    NEW.centroid = ST_Centroid(NEW.bbox);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_roi_centroid ON regions_of_interest;
CREATE TRIGGER trg_roi_centroid
BEFORE INSERT OR UPDATE OF bbox ON regions_of_interest
FOR EACH ROW EXECUTE FUNCTION fn_compute_roi_centroid();

-- =============================================================================
-- SEED DATA: Pipeline stages (required for FK references to work)
-- =============================================================================
INSERT INTO processing_stages (stage_name, stage_code, stage_order, description, timeout_minutes, retry_count, retry_delay_sec)
VALUES
    ('DOWNLOAD',          'DL',  1, 'Sentinel-1 scene discovery and download from Copernicus Hub', 120, 3, 60),
    ('CROP',              'CR',  2, 'Spatial subsetting to Region of Interest bounding box',       30,  2, 30),
    ('LEE_FILTER',        'LF',  3, 'SAR speckle reduction using Lee adaptive filter',            45,  2, 30),
    ('COG_EXPORT',        'CE',  4, 'Cloud-Optimized GeoTIFF normalization and export',           30,  2, 30),
    ('ORCHESTRATE',       'OR',  5, 'Pipeline orchestration, checkpointing, and retry management', 10,  1, 10),
    ('QUALITY_ANALYTICS', 'QA',  6, 'Quality metrics computation and visualization',              30,  2, 30),
    ('FUSION',            'FS',  7, 'Multi-modal HDF5 feature stack fusion (Sentinel-1 + MODIS + GPM)', 60, 2, 30),
    ('GOLD_EXPORT',       'GE',  8, 'Per-source Cloud-Optimized GeoTIFF export untuk tier GOLD',  45,  2, 30),
    ('PREVIEW',           'PV',  9, 'Render PNG preview (grayscale + colored) dari tier GOLD, sebelum FUSION', 15, 1, 15)
ON CONFLICT (stage_name) DO NOTHING;

-- Seed: Default processing rules per stage
INSERT INTO processing_rules (stage_id, rule_name, rule_code, rule_type, description, threshold_value, threshold_unit, operator, action_on_fail)
SELECT s.stage_id, r.rule_name, r.rule_code, r.rule_type::rule_type_enum, r.description, r.threshold_value, r.threshold_unit, r.operator, r.action_on_fail
FROM processing_stages s
JOIN (VALUES
    ('DOWNLOAD',          'Max Download Size',       'DL_MAX_SIZE',        'THRESHOLD',     'Maximum allowed file size for download',            5000,  'MB',      '<',  'WARN'),
    ('DOWNLOAD',          'Min Resolution',          'DL_MIN_RES',         'VALIDATION',    'Minimum spatial resolution required',              10,    'm',       '<=', 'FAIL'),
    ('CROP',              'Valid Overlap',            'CR_OVERLAP',         'THRESHOLD',     'Minimum overlap % between scene and ROI bbox',     10,    '%',       '>',  'FAIL'),
    ('CROP',              'Output Size Check',        'CR_OUTPUT_SIZE',     'VALIDATION',    'Cropped output must not be empty',                 0,     'pixels',  '>',  'FAIL'),
    ('LEE_FILTER',        'Max Nodata Percent',       'LF_NODATA_PCT',      'THRESHOLD',     'Maximum nodata percentage after filtering',        30,    '%',       '<',  'WARN'),
    ('LEE_FILTER',        'Backscatter Range',        'LF_BACKSCATTER',     'THRESHOLD',     'Valid backscatter range for Sentinel-1 IW',       -35,   'dB',      '>',  'WARN'),
    ('COG_EXPORT',        'COG Compliance',           'CE_COG_VALID',       'VALIDATION',    'Output must pass GDAL COG validation',             NULL,  NULL,      NULL, 'FAIL'),
    ('COG_EXPORT',        'Compression Ratio',        'CE_COMPRESS_RATIO',  'THRESHOLD',     'Minimum compression ratio for storage efficiency', 1.5,  'ratio',   '>',  'WARN'),
    ('QUALITY_ANALYTICS', 'Min Quality Score',        'QA_MIN_SCORE',       'THRESHOLD',     'Minimum composite quality score to mark as PASS',  60,   'score',   '>',  'FAIL'),
    ('QUALITY_ANALYTICS', 'Radiometric Consistency',  'QA_RADIOMETRIC',     'VALIDATION',    'Backscatter statistics must be within valid range', NULL,  NULL,      NULL, 'WARN')
) AS r(stage_name, rule_name, rule_code, rule_type, description, threshold_value, threshold_unit, operator, action_on_fail)
ON s.stage_name = r.stage_name
ON CONFLICT (rule_code) DO NOTHING;

-- =============================================================================
-- VIEWS: Convenience queries for common access patterns
-- =============================================================================

-- View: Latest production-ready scenes with quality scores
CREATE OR REPLACE VIEW vw_latest_scenes_quality AS
SELECT
    ss.scene_id,
    ss.product_identifier,
    ss.acquisition_datetime,
    ss.orbit_direction,
    ss.polarization_vv,
    ss.polarization_vh,
    roi.name           AS region_name,
    roi.region_code,
    qm.band_name,
    qm.quality_score,
    qm.quality_flag,
    qm.nodata_percent,
    qm.backscatter_mean_db,
    dp.product_tier,
    dp.file_path       AS product_path,
    dp.file_size_mb
FROM satellite_scenes    ss
JOIN regions_of_interest roi ON ss.region_id   = roi.region_id
LEFT JOIN data_products  dp  ON dp.scene_id    = ss.scene_id
                             AND dp.is_latest  = TRUE
                             AND dp.product_tier = 'GOLD'
LEFT JOIN quality_metrics qm ON qm.scene_id   = ss.scene_id
                             AND qm.product_id = dp.product_id
WHERE ss.is_available = TRUE
ORDER BY ss.acquisition_datetime DESC;

COMMENT ON VIEW vw_latest_scenes_quality IS 'Latest GOLD-tier product per scene with quality metrics. Primary API query view.';

-- View: Pipeline execution summary per scene
CREATE OR REPLACE VIEW vw_pipeline_status AS
SELECT
    ss.scene_id,
    ss.product_identifier,
    ss.acquisition_datetime,
    ps.stage_name,
    ps.stage_order,
    pj.status          AS job_status,
    pj.started_at,
    pj.completed_at,
    pj.duration_seconds,
    pj.error_message
FROM satellite_scenes  ss
JOIN processing_jobs   pj ON pj.scene_id = ss.scene_id
JOIN processing_stages ps ON ps.stage_id = pj.stage_id
ORDER BY ss.acquisition_datetime DESC, ps.stage_order;

COMMENT ON VIEW vw_pipeline_status IS 'Full ETL pipeline execution status per scene × stage.';

-- =============================================================================
-- SCHEMA VERSION TRACKING
-- =============================================================================
CREATE TABLE IF NOT EXISTS schema_migrations (
    migration_id    SERIAL      PRIMARY KEY,
    version         VARCHAR(20) NOT NULL UNIQUE,
    description     TEXT        NOT NULL,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by      VARCHAR(100) DEFAULT current_user
);

INSERT INTO schema_migrations (version, description)
VALUES ('1.0.0', 'Initial schema: 11 master tables, PostGIS, TimescaleDB hypertables, triggers, seed data')
ON CONFLICT (version) DO NOTHING;

-- =============================================================================
-- END OF SCHEMA
-- =============================================================================
-- Summary:
--   Tables   : 11 master + 1 migration tracker = 12 total
--   Hypertables: satellite_scenes, api_access_logs, alert_events (if TimescaleDB)
--   Indexes  : 50+ indexes across all tables
--   Triggers : updated_at auto-maintenance + ROI centroid auto-compute
--   Views    : vw_latest_scenes_quality, vw_pipeline_status
--   Seed Data: 6 processing stages + 10 processing rules
-- =============================================================================
