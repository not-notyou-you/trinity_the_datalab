-- 025_live_monitoring.sql
--
-- Live Monitoring (LIVE_MONITORING.md): pemantauan otomatis per Daerah Live.
--
-- Seluruhnya ADITIF:
--   * datasets.dataset_kind menerima nilai baru 'LIVE_AREA'. CHECK lama dibuat
--     ulang dengan daftar yang lebih luas; 'STANDARD' dan 'LIVE' tetap sah, dan
--     indeks unik uq_single_live_dataset (hanya untuk 'LIVE') tidak disentuh.
--   * tiga tabel baru: live_areas, live_scenes, live_events.
--
-- Satu Daerah Live = satu baris datasets (dataset_kind='LIVE_AREA') yang
-- diproses pipeline biasa (run_dataset_job) + satu baris live_areas yang
-- memegang aturan bisnisnya (retensi, forecast).
--
-- live_scenes dan live_events adalah LOG: tidak pernah dihapus, termasuk saat
-- scene kena retensi atau daerahnya dihapus. Karena itu tidak ada FK CASCADE
-- dari keduanya ke datasets, dan live_areas.dataset_id memakai SET NULL.
--
-- Data Live lama (dataset_kind='LIVE', live_dataset_sources) tidak diubah.

BEGIN;

ALTER TABLE datasets DROP CONSTRAINT IF EXISTS chk_dataset_kind;
ALTER TABLE datasets ADD CONSTRAINT chk_dataset_kind
    CHECK (dataset_kind IN ('STANDARD', 'LIVE', 'LIVE_AREA'));

CREATE TABLE IF NOT EXISTS live_areas (
    area_id             SERIAL          PRIMARY KEY,
    dataset_id          INTEGER         REFERENCES datasets(dataset_id) ON DELETE SET NULL,
    name                VARCHAR(255)    NOT NULL,
    region_id           INTEGER         REFERENCES regions_of_interest(region_id) ON DELETE SET NULL,
    location_label      VARCHAR(255),
    bbox_wkt            TEXT            NOT NULL,
    retention           SMALLINT        NOT NULL DEFAULT 6,
    enabled             BOOLEAN         NOT NULL DEFAULT TRUE,
    -- BACKFILLING | ACTIVE | RUNNING | ERROR | DELETED
    status              VARCHAR(20)     NOT NULL DEFAULT 'BACKFILLING',
    status_message      TEXT,
    last_checked_at     TIMESTAMPTZ,
    forecast            JSONB           NOT NULL DEFAULT '{}'::jsonb,
    forecast_updated_at TIMESTAMPTZ,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    deleted_at          TIMESTAMPTZ,
    CONSTRAINT chk_live_area_retention CHECK (retention BETWEEN 1 AND 12),
    CONSTRAINT chk_live_area_status CHECK (
        status IN ('BACKFILLING', 'ACTIVE', 'RUNNING', 'ERROR', 'DELETED'))
);

CREATE INDEX IF NOT EXISTS idx_live_areas_active
    ON live_areas (area_id) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS live_scenes (
    live_scene_id       BIGSERIAL       PRIMARY KEY,
    -- Bukan FK: log harus hidup lebih lama dari apa pun yang dirujuknya.
    area_id             INTEGER         NOT NULL,
    dataset_id          INTEGER,
    scene_date          DATE            NOT NULL,
    s1_product_ids      TEXT[]          NOT NULL DEFAULT ARRAY[]::TEXT[],
    -- PROCESSING | READY | PARTIAL | FAILED | DELETED
    status              VARCHAR(20)     NOT NULL DEFAULT 'PROCESSING',
    -- {sentinel1|modis|gpm: {status: OK|FAILED|UNAVAILABLE, detail, matched_date, nearest}}
    source_status       JSONB           NOT NULL DEFAULT '{}'::jsonb,
    metrics             JSONB           NOT NULL DEFAULT '{}'::jsonb,
    interpretations     JSONB           NOT NULL DEFAULT '{}'::jsonb,
    area_status         JSONB           NOT NULL DEFAULT '{}'::jsonb,
    previews            JSONB           NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    deleted_at          TIMESTAMPTZ,
    delete_reason       TEXT,
    deleted_files       JSONB           NOT NULL DEFAULT '[]'::jsonb,
    freed_bytes         BIGINT          NOT NULL DEFAULT 0,
    CONSTRAINT uq_live_scene_area_date UNIQUE (area_id, scene_date),
    CONSTRAINT chk_live_scene_status CHECK (
        status IN ('PROCESSING', 'READY', 'PARTIAL', 'FAILED', 'DELETED'))
);

CREATE INDEX IF NOT EXISTS idx_live_scenes_area_date
    ON live_scenes (area_id, scene_date DESC);

CREATE TABLE IF NOT EXISTS live_events (
    event_id            BIGSERIAL       PRIMARY KEY,
    area_id             INTEGER         NOT NULL,
    scene_date          DATE,
    step                VARCHAR(40)     NOT NULL,
    status              VARCHAR(20)     NOT NULL,
    message             TEXT            NOT NULL,
    details             JSONB           NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_live_events_area_time
    ON live_events (area_id, created_at DESC);

COMMIT;
