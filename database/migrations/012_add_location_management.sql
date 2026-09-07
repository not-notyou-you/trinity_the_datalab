-- database/migrations/012_add_location_management.sql
-- Locations management: regions_of_interest becomes the single source of truth for
-- the "Pilih Lokasi" picker (menggantikan config/config_locations.json + area_presets
-- di config/config.json). Menambahkan asal-usul baris (source) dan soft-delete.

-- ---------------------------------------------------------------------------
-- 1. Kolom baru
-- ---------------------------------------------------------------------------
-- source: dari mana baris ini berasal. Default 'USER' supaya baris yang dibuat
--         lewat POST /api/regions otomatis tertandai sebagai buatan pengguna.
ALTER TABLE regions_of_interest
    ADD COLUMN IF NOT EXISTS source     VARCHAR(20) NOT NULL DEFAULT 'USER',
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

ALTER TABLE regions_of_interest DROP CONSTRAINT IF EXISTS chk_roi_source;
ALTER TABLE regions_of_interest
    ADD CONSTRAINT chk_roi_source CHECK (source IN ('SEEDER', 'USER', 'GEOCODE'));

COMMENT ON COLUMN regions_of_interest.source IS
    'SEEDER = bawaan sistem (tidak bisa dihapus), USER = ditambahkan lewat UI, GEOCODE = auto-dibuat location_resolver dari Nominatim.';
COMMENT ON COLUMN regions_of_interest.deleted_at IS
    'Waktu soft-delete. NULL = aktif. Baris tidak pernah dihapus fisik karena satellite_scenes/datasets mereferensikan region_id (ON DELETE RESTRICT).';

-- ---------------------------------------------------------------------------
-- 2. Backfill asal-usul baris yang sudah ada
-- ---------------------------------------------------------------------------
-- Baris dari database/seed_data.sql + migrasi 005 = bawaan sistem.
UPDATE regions_of_interest
   SET source = 'SEEDER'
 WHERE source = 'USER'
   AND region_code IN (
       'JABODTK', 'JKT', 'JKT_TEST',
       'KOTA_BOGOR', 'KAB_BOGOR', 'KOTA_DEPOK',
       'KOTA_TANGERANG', 'KAB_TANGERANG', 'KOTA_BEKASI', 'KAB_BEKASI'
   );

-- Baris yang dibuat otomatis oleh etl/location_resolver.py memakai prefiks 'AUTO'.
UPDATE regions_of_interest
   SET source = 'GEOCODE'
 WHERE source = 'USER'
   AND region_code LIKE 'AUTO%';

-- Selaraskan soft-delete untuk baris yang sudah terlanjur dinonaktifkan.
UPDATE regions_of_interest
   SET deleted_at = COALESCE(updated_at, NOW())
 WHERE is_active = FALSE
   AND deleted_at IS NULL;

-- ---------------------------------------------------------------------------
-- 3. Migrasi lokasi dari config/config_locations.json yang belum masuk DB
-- ---------------------------------------------------------------------------
-- JKT_TEST ada di config_locations.json tapi tidak pernah ikut ter-seed. Ini AOI
-- kecil untuk smoke test pipeline, jadi tetap dibutuhkan sebagai default.
INSERT INTO regions_of_interest
    (region_code, name, description, bbox, area_km2, admin_level, country_code, is_active, source)
VALUES (
    'JKT_TEST',
    'DKI Jakarta - Area Uji',
    'Sepersembilan bbox DKI Jakarta (petak tengah-utara), untuk smoke test pipeline sebelum run area penuh',
    ST_GeomFromText('POLYGON((106.78 -6.22, 106.87 -6.22, 106.87 -6.07, 106.78 -6.07, 106.78 -6.22))', 4326),
    110.25, 3, 'ID', TRUE, 'SEEDER'
) ON CONFLICT (region_code) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 4. Index untuk pencarian nama (search bar) + filter soft-delete
-- ---------------------------------------------------------------------------
-- Pencarian di UI memakai lower(name) LIKE '%q%'. Index lower(name) menolong
-- prefix match; sisanya scan, dan jumlah baris tabel ini memang kecil.
CREATE INDEX IF NOT EXISTS idx_roi_name_lower ON regions_of_interest (lower(name));
CREATE INDEX IF NOT EXISTS idx_roi_source     ON regions_of_interest (source);
CREATE INDEX IF NOT EXISTS idx_roi_not_deleted
    ON regions_of_interest (deleted_at) WHERE deleted_at IS NULL;

INSERT INTO schema_migrations (version, description)
VALUES ('012', 'Locations management: regions_of_interest.source + deleted_at (soft-delete), seed JKT_TEST dari config_locations.json, index pencarian nama')
ON CONFLICT (version) DO NOTHING;
