-- database/migrations/003_add_multisensor.sql

CREATE TABLE IF NOT EXISTS nasa_scenes (
    nasa_scene_id       BIGSERIAL PRIMARY KEY,
    source               VARCHAR(20) NOT NULL,
    tile_id              VARCHAR(10) NOT NULL,
    product_short_name   VARCHAR(50) NOT NULL,
    acquisition_date     DATE NOT NULL,
    region_id            INTEGER NOT NULL REFERENCES regions_of_interest(region_id) ON DELETE RESTRICT,
    raw_file_path        TEXT,
    download_url         TEXT,
    is_available          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_nasa_scene UNIQUE (source, tile_id, product_short_name, acquisition_date)
);

CREATE INDEX IF NOT EXISTS idx_nasa_scenes_date ON nasa_scenes (acquisition_date DESC);
CREATE INDEX IF NOT EXISTS idx_nasa_scenes_tile ON nasa_scenes (tile_id);

CREATE TABLE IF NOT EXISTS fusion_products (
    fusion_id            BIGSERIAL PRIMARY KEY,
    feature_date         DATE NOT NULL,
    region_id            INTEGER NOT NULL REFERENCES regions_of_interest(region_id) ON DELETE RESTRICT,
    s1_scene_id          INTEGER REFERENCES satellite_scenes(scene_id) ON DELETE SET NULL,
    modis_scene_id       BIGINT REFERENCES nasa_scenes(nasa_scene_id) ON DELETE SET NULL,
    gpm_scene_id         BIGINT REFERENCES nasa_scenes(nasa_scene_id) ON DELETE SET NULL,
    days_since_s1        INTEGER NOT NULL,
    feature_stack_path   TEXT NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fusion_date_region UNIQUE (feature_date, region_id)
);

CREATE INDEX IF NOT EXISTS idx_fusion_date ON fusion_products (feature_date DESC);

INSERT INTO schema_migrations (version, description)
VALUES ('003', 'Add nasa_scenes and fusion_products tables for MODIS/GPM multi-sensor fusion')
ON CONFLICT (version) DO NOTHING;