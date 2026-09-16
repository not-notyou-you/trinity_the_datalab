-- database/migrations/021_fusion_products_dataset_scope.sql
-- =============================================================================
-- fusion_products per dataset + pembersihan job yang tertinggal
-- =============================================================================
--
-- APA YANG BERUBAH
-- 1. fusion_products.dataset_id + kunci unik (dataset_id, feature_date,
--    processing_level).
--
--    Kunci lama (feature_date, region_id, processing_level) tidak mengenal
--    dataset. Dua dataset atas AOI dan tanggal yang sama -- kasus normal saat
--    membandingkan strategi fusi (try1 HYBRID / try2 FULL_COVERAGE / try3
--    CO_OCCURRENCE, 2026-09-16) -- berbagi SATU baris: ketiganya menulis
--    fusion_id=43 untuk 2025-01-11 dan dataset yang selesai terakhir menimpa
--    feature_stack_path, fusion_strategy, dan offset milik dua lainnya.
--
--    region_id sengaja keluar dari kunci: satu dataset punya satu AOI, dan
--    scene S1 yang dipakai bersama antar-dataset membawa region_id dataset
--    yang PERTAMA mendaftarkannya, sehingga satu dataset bisa punya baris
--    dengan region_id berbeda (hari ber-S1 vs hari tanpa-S1).
--
-- 2. Pembersihan status processing_jobs yang tertinggal oleh bug ETL yang
--    diperbaiki bersamaan dengan migrasi ini:
--      * job DOWNLOAD registrasi aux MODIS/GPM yang dibuat tapi tidak pernah
--        dijalankan (QUEUED selamanya) -> SUCCESS. Produknya memang sudah
--        terdaftar; yang tidak pernah terjadi hanya penutupan job-nya.
--      * job FUSION yang jatuh KeyError 'vv_product_id' setelah HDF5 ditulis
--        (RUNNING selamanya) -> FAILED.
--    Keduanya hanya dijalankan kalau TIDAK ADA dataset_jobs yang masih aktif
--    (status selain COMPLETED/FAILED/CANCELLED): dengan begitu tidak ada job
--    yang benar-benar sedang berjalan yang ikut ditutup. Kalau ada job aktif,
--    bagian ini dilewati tanpa error -- jalankan ulang migrasinya nanti.
--
-- BACKWARD COMPATIBILITY
--   * dataset_id di-backfill dari feature_stack_path
--     (data/datasets/{dataset_id}_{slug}/...), satu-satunya tempat identitas
--     dataset tercatat di baris lama. Baris yang path-nya tidak bisa diurai
--     atau datasetnya sudah dihapus tetap NULL; NULL tidak pernah bertabrakan
--     di kunci unik PostgreSQL, jadi baris lama tidak menolak migrasi.
--   * Baris lama sudah unik per (feature_date, region_id, level); dalam satu
--     dataset satu AOI, jadi juga unik per (dataset_id, feature_date, level).
--   * Idempoten: aman dijalankan ulang.

BEGIN;

-- 1. Kolom + backfill ---------------------------------------------------------
ALTER TABLE fusion_products
    ADD COLUMN IF NOT EXISTS dataset_id INTEGER;

UPDATE fusion_products fp
SET    dataset_id = sub.dataset_id
FROM (
    SELECT fusion_id,
           (substring(feature_stack_path FROM '(?:^|[\\/])datasets[\\/](\d+)_'))::INTEGER AS dataset_id
    FROM   fusion_products
    WHERE  dataset_id IS NULL
) sub
WHERE  fp.fusion_id = sub.fusion_id
  AND  sub.dataset_id IS NOT NULL
  AND  EXISTS (SELECT 1 FROM datasets d WHERE d.dataset_id = sub.dataset_id);

ALTER TABLE fusion_products
    DROP CONSTRAINT IF EXISTS fk_fusion_products_dataset;
ALTER TABLE fusion_products
    ADD CONSTRAINT fk_fusion_products_dataset
    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE;

-- 2. Tukar kunci unik ---------------------------------------------------------
ALTER TABLE fusion_products
    DROP CONSTRAINT IF EXISTS uq_fusion_date_region_level;
ALTER TABLE fusion_products
    DROP CONSTRAINT IF EXISTS uq_fusion_dataset_date_level;
ALTER TABLE fusion_products
    ADD CONSTRAINT uq_fusion_dataset_date_level
    UNIQUE (dataset_id, feature_date, processing_level);

CREATE INDEX IF NOT EXISTS idx_fusion_dataset_date
    ON fusion_products (dataset_id, feature_date);

COMMENT ON COLUMN fusion_products.dataset_id IS
    'Dataset pemilik stack ini. Bagian kunci unik: dua dataset atas AOI & tanggal yang sama punya baris masing-masing.';

-- 3. Pembersihan job yang tertinggal -----------------------------------------
UPDATE processing_jobs pj
SET    status       = 'SUCCESS',
       started_at   = COALESCE(pj.started_at, pj.queued_at),
       completed_at = COALESCE(pj.completed_at, pj.queued_at)
FROM   processing_stages ps
WHERE  ps.stage_id = pj.stage_id
  AND  ps.stage_name = 'DOWNLOAD'
  AND  pj.status = 'QUEUED'
  AND  pj.parameters_json ? 'source'
  AND  pj.parameters_json->>'source' IN ('MODIS', 'GPM')
  AND  NOT EXISTS (
         SELECT 1 FROM dataset_jobs dj
         WHERE  dj.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
       );

UPDATE processing_jobs pj
SET    status        = 'FAILED',
       completed_at  = NOW(),
       error_code    = COALESCE(pj.error_code, 'STALE_RUNNING'),
       error_message = COALESCE(pj.error_message,
                        'Ditutup migrasi 021: job FUSION tertinggal RUNNING (bug KeyError vv_product_id). Jalankan ulang dataset untuk membangun ulang stack.')
FROM   processing_stages ps
WHERE  ps.stage_id = pj.stage_id
  AND  ps.stage_name = 'FUSION'
  AND  pj.status = 'RUNNING'
  AND  NOT EXISTS (
         SELECT 1 FROM dataset_jobs dj
         WHERE  dj.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
       );

INSERT INTO schema_migrations (version, description)
VALUES ('021', 'fusion_products.dataset_id + uq_fusion_dataset_date_level; tutup processing_jobs aux QUEUED & FUSION RUNNING yang tertinggal')
ON CONFLICT (version) DO NOTHING;

COMMIT;
