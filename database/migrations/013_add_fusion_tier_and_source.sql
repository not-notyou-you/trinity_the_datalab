-- database/migrations/013_add_fusion_tier_and_source.sql
--
-- Refactor tier: RAW -> BRONZE -> SILVER -> GOLD -> FUSION, dengan tiap
-- produk dilabeli source-nya.
--
-- Sebelumnya GOLD berarti "HDF5 fusion stack" (lihat migrasi 010) dan tidak
-- ada produk GOLD per-source. Sekarang:
--   GOLD   = produk analysis-ready per-source (COG per band, per sensor)
--   FUSION = HDF5 multi-modal gabungan semua source (satu file per tanggal)
--
-- Kolom `source` dulu cuma tersirat lewat product_type (LEE_FILTERED itu S1,
-- MODIS_FLOOD itu MODIS, dst). Sekarang eksplisit supaya filter
-- "GOLD punya MODIS saja" jadi satu index scan, bukan pencocokan string
-- product_type yang harus diperbarui tiap kali ada product_type baru.
--
-- PENTING: jangan jalankan file ini dengan `psql --single-transaction`.
-- ALTER TYPE ... ADD VALUE harus commit dulu sebelum nilai barunya boleh
-- dipakai di UPDATE di bawah; psql default (autocommit per statement)
-- sudah benar.
--
--   psql -U postgres -d sentinel1_flood -f database/migrations/013_add_fusion_tier_and_source.sql

-- ---------------------------------------------------------------------------
-- 1. Tier FUSION
-- ---------------------------------------------------------------------------
ALTER TYPE product_tier_enum ADD VALUE IF NOT EXISTS 'FUSION' AFTER 'GOLD';

-- ---------------------------------------------------------------------------
-- 2. Kolom source di data_products
-- ---------------------------------------------------------------------------
ALTER TABLE data_products ADD COLUMN IF NOT EXISTS source VARCHAR(20);

-- Backfill dari product_type yang sudah ada. FUSION_H5 dilabeli 'FUSION'
-- (bukan SENTINEL1) walaupun row-nya menempel ke scene_id S1: isinya
-- gabungan tiga sensor, jadi melabelinya SENTINEL1 akan bikin filter
-- "?source=sentinel1" ikut menarik file fusion.
UPDATE data_products SET source = CASE
    WHEN product_type = 'MODIS_FLOOD'                          THEN 'MODIS'
    WHEN product_type LIKE 'MODIS%'                            THEN 'MODIS'
    WHEN product_type = 'GPM_RAINFALL'                         THEN 'GPM'
    WHEN product_type LIKE 'GPM%'                              THEN 'GPM'
    WHEN product_type = 'FUSION_H5'                            THEN 'FUSION'
    ELSE 'SENTINEL1'
END
WHERE source IS NULL;

ALTER TABLE data_products ALTER COLUMN source SET NOT NULL;
ALTER TABLE data_products ALTER COLUMN source SET DEFAULT 'SENTINEL1';

ALTER TABLE data_products DROP CONSTRAINT IF EXISTS chk_dprods_source;
ALTER TABLE data_products ADD CONSTRAINT chk_dprods_source
    CHECK (source IN ('SENTINEL1', 'MODIS', 'GPM', 'FUSION'));

-- Filter utama API: "produk tier X dari source Y untuk dataset Z".
CREATE INDEX IF NOT EXISTS idx_dprods_tier_source
    ON data_products (product_tier, source);
CREATE INDEX IF NOT EXISTS idx_dprods_dataset_tier_source
    ON data_products (dataset_id, product_tier, source) WHERE is_latest = TRUE;

-- ---------------------------------------------------------------------------
-- 3. Pindahkan produk fusion yang sudah ada dari GOLD ke FUSION
-- ---------------------------------------------------------------------------
-- Row FUSION_H5 lama dicatat sebagai tier GOLD karena waktu itu GOLD memang
-- berarti fusion. Sekarang GOLD punya arti sendiri, jadi row-nya dipindah.
-- file_path-nya ikut dipindah ke folder fusion/ oleh
-- etl/migrate_data_structure.py — jalankan script itu SETELAH migrasi ini.
UPDATE data_products
   SET product_tier = 'FUSION'
 WHERE product_type = 'FUSION_H5'
   AND product_tier = 'GOLD';

-- ---------------------------------------------------------------------------
-- 4. Stage GOLD_EXPORT
-- ---------------------------------------------------------------------------
-- Tahap yang mengubah produk SILVER per-source jadi COG analysis-ready di
-- GOLD (etl/module4_gold_export.py). Namanya bukan COG_EXPORT: row itu masih
-- direferensikan processing_jobs/data_lineage historis dari arsitektur lama
-- (lihat migrasi 010), dan artinya beda — COG_EXPORT dulu cuma Sentinel-1.
INSERT INTO processing_stages (stage_name, stage_code, stage_order, description, timeout_minutes, retry_count, retry_delay_sec)
VALUES
    ('GOLD_EXPORT', 'GE', 8, 'Per-source Cloud-Optimized GeoTIFF export (Sentinel-1 / MODIS / GPM) untuk tier GOLD', 45, 2, 30)
ON CONFLICT (stage_name) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 5. Komentar
-- ---------------------------------------------------------------------------
COMMENT ON COLUMN data_products.product_tier IS
    'Lakehouse tier: RAW (original), BRONZE (cropped ke AOI), SILVER (processed per-source), GOLD (analysis-ready per-source COG), FUSION (HDF5 multi-modal gabungan).';
COMMENT ON COLUMN data_products.source IS
    'Sensor asal produk: SENTINEL1 | MODIS | GPM, atau FUSION untuk stack gabungan. Sama dengan level {source} di path on-disk (etl/folder_manager.py).';

INSERT INTO schema_migrations (version, description)
VALUES ('013', 'Add FUSION tier + data_products.source column; GOLD becomes per-source analysis-ready products')
ON CONFLICT (version) DO NOTHING;
