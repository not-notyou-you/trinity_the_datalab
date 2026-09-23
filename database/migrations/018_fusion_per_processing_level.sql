-- database/migrations/018_fusion_per_processing_level.sql
-- =============================================================================
-- Fusi per-processing_level: satu stack HDF5 per (tanggal, region, level)
-- =============================================================================
--
-- APA YANG BERUBAH
-- Migrasi 017 sudah menambahkan `fusion_products.processing_level`, tapi kunci
-- unik tabelnya masih `(feature_date, region_id)` -- warisan model lama yang
-- hanya mengenal satu jalur pemrosesan.
--
-- Model per-satelit membolehkan sebuah sumber diminta RAW **dan** PROCESSED
-- sekaligus. DOCS/PIPELINE.md ("Which input tier does fusion use?") menyatakan
-- konfigurasi itu menghasilkan DUA fusion run untuk tanggal yang sama: satu
-- dari BRONZE, satu dari GOLD. Dengan kunci lama, baris kedua bertabrakan
-- dengan yang pertama dan ETL menimpanya -- persis membuang sisi pembanding
-- yang jadi alasan konfigurasi itu dipilih.
--
-- Kunci baru karena itu memasukkan processing_level.
--
-- BACKWARD COMPATIBILITY
--   * Baris lama semuanya processing_level='PROCESSED' (DEFAULT dari 017),
--     jadi kunci baru tidak pernah menolak data yang sudah ada: setiap
--     (feature_date, region_id) yang tadinya unik tetap unik sebagai
--     (feature_date, region_id, 'PROCESSED').
--   * Kolom di-backfill dulu supaya baris pra-017 yang NULL tidak lolos dari
--     kunci unik (di PostgreSQL NULL tidak pernah sama dengan NULL, jadi
--     beberapa baris NULL bisa berdampingan dan menghidupkan lagi bug yang
--     justru ditutup migrasi ini).
--   * Idempoten: aman dijalankan ulang.

BEGIN;

-- 1. Tidak boleh ada NULL sebelum kolomnya masuk kunci unik.
UPDATE fusion_products
SET    processing_level = 'PROCESSED'
WHERE  processing_level IS NULL;

ALTER TABLE fusion_products
    ALTER COLUMN processing_level SET DEFAULT 'PROCESSED';
ALTER TABLE fusion_products
    ALTER COLUMN processing_level SET NOT NULL;

-- 2. Tukar kunci uniknya.
ALTER TABLE fusion_products
    DROP CONSTRAINT IF EXISTS uq_fusion_date_region;
ALTER TABLE fusion_products
    DROP CONSTRAINT IF EXISTS uq_fusion_date_region_level;
ALTER TABLE fusion_products
    ADD CONSTRAINT uq_fusion_date_region_level
    UNIQUE (feature_date, region_id, processing_level);

COMMENT ON COLUMN fusion_products.processing_level IS
    'Level input yang dipakai stack ini (RAW = dibaca dari BRONZE, PROCESSED = dari GOLD). Bagian dari kunci unik: satu tanggal bisa punya dua stack kalau datasetnya meminta sebuah sumber di kedua level.';

CREATE INDEX IF NOT EXISTS idx_fusion_region_date_level
    ON fusion_products (region_id, feature_date, processing_level);

-- 3. datasets.preview_options: NULL tidak boleh lagi bermakna ganda.
-- Pipeline sekarang membaca kolom ini (tahap PREVIEW merender hanya varian
-- yang disebut). Array KOSONG berarti "user tidak mau preview apa pun", jadi
-- NULL pada baris pra-017 harus dibedakan darinya -- kalau dibiarkan, baris
-- lama akan terbaca sebagai "tidak mau preview" dan diam-diam berhenti
-- menghasilkan PNG.
UPDATE datasets
SET    preview_options = ARRAY['GRAYSCALE', 'COLORED', 'COMPOSITE']::TEXT[]
WHERE  preview_options IS NULL;

ALTER TABLE datasets
    ALTER COLUMN preview_options SET DEFAULT ARRAY['GRAYSCALE', 'COLORED', 'COMPOSITE']::TEXT[];
ALTER TABLE datasets
    ALTER COLUMN preview_options SET NOT NULL;

-- 4. data_products.band_name perlu muat nama band tier FUSION per level.
-- Stack fusion sekarang dinamai "FUSION_RAW" / "FUSION_PROCESSED" (16 karakter)
-- supaya dedup is_latest -- yang berjalan atas (scene_id, band_name,
-- product_tier, dataset_id) -- tidak membuat stack RAW menandai dirinya usang
-- begitu stack PROCESSED tanggal yang sama didaftarkan. Dengan VARCHAR(10),
-- INSERT-nya gagal dan seluruh tahap FUSION jatuh setelah HDF5-nya terlanjur
-- ditulis ke disk.
--
-- Melebarkan VARCHAR tidak pernah menolak data yang sudah ada dan tidak
-- menulis ulang tabel, jadi aman dijalankan kapan saja. quality_metrics.
-- band_name sengaja DIBIARKAN (10): isinya cuma VV/VH.
ALTER TABLE data_products
    ALTER COLUMN band_name TYPE VARCHAR(20);

COMMENT ON COLUMN data_products.band_name IS
    'Band/lapisan artefak ini. Untuk tier FUSION isinya FUSION_RAW atau FUSION_PROCESSED -- level ikut ke nama supaya dua stack tanggal yang sama bisa berdampingan tanpa saling menandai usang.';

INSERT INTO schema_migrations (version, description)
VALUES ('018', 'Fusion per processing_level: uq_fusion_date_region -> uq_fusion_date_region_level, processing_level NOT NULL; datasets.preview_options NOT NULL; data_products.band_name VARCHAR(20)')
ON CONFLICT (version) DO NOTHING;

COMMIT;
