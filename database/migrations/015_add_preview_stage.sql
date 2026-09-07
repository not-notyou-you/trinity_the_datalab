-- database/migrations/015_add_preview_stage.sql
-- =============================================================================
-- Tier PREVIEW: render PNG turunan dari GOLD (etl/module10_generate_preview.py)
-- =============================================================================
--
-- Migrasi ini SENGAJA hanya menambah satu baris di processing_stages, tanpa
-- menyentuh enum product_tier_enum maupun kolom data_products.
--
-- Alasannya: PREVIEW bukan mata rantai lineage RAW -> BRONZE -> SILVER ->
-- GOLD -> FUSION. Isinya PNG turunan yang bisa dibangun ulang kapan saja dari
-- gold/, bukan produk data yang di-checksum, dilacak provenance-nya, atau
-- dipakai ulang tahap berikutnya. Mendaftarkannya sebagai product_tier baru
-- akan:
--   * memaksa perubahan enum yang menyentuh setiap query lama yang meng-cast
--     product_tier,
--   * membuat 8+ baris data_products per scene yang tidak pernah dibaca
--     siapa pun kecuali UI, padahal UI membacanya lebih murah langsung dari
--     disk lewat preview_metadata.json,
--   * membuat compute_tiers_to_delete menghapus preview setiap kali user
--     tidak menyebut PREVIEW di required_tiers -- yaitu pada SEMUA dataset
--     yang sudah ada.
--
-- Sumber kebenaran isi tier ini adalah sidecar JSON di disk
-- (preview/{tanggal}/preview_metadata.json), dan ukurannya tetap terhitung di
-- folder_manager.storage_breakdown karena "preview" ada di fm.TIERS.
--
-- Migrasi ini opsional untuk instalasi lama: tanpanya, tahap PREVIEW tetap
-- berjalan dan tetap menulis PNG-nya, cuma baris processing_jobs-nya tidak
-- terdaftar. Orchestrator tidak memanggil insert_processing_job() untuk
-- PREVIEW justru supaya sifat opsional itu terjaga.

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. Stage PREVIEW
-- ---------------------------------------------------------------------------
-- stage_order 9 = urutan pendaftaran, bukan urutan eksekusi. Kolom itu sudah
-- lama tidak mencerminkan urutan jalan pipeline (FUSION order 7 padahal jalan
-- setelah GOLD_EXPORT order 8, lihat migrasi 013); urutan eksekusi yang
-- sesungguhnya hidup di etl/dataset_manager.py:STAGE_TIER_INDEX.
INSERT INTO processing_stages (stage_name, stage_code, stage_order, description, timeout_minutes, retry_count, retry_delay_sec)
VALUES
    ('PREVIEW', 'PV', 9, 'Render PNG preview (grayscale + colored) dari tier GOLD, sebelum FUSION', 15, 1, 15)
ON CONFLICT (stage_name) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 2. Komentar
-- ---------------------------------------------------------------------------
COMMENT ON TABLE processing_stages IS
    'Master tahap pipeline. Urutan eksekusi sebenarnya: DOWNLOAD -> CROP -> LEE_FILTER -> QUALITY_ANALYTICS -> GOLD_EXPORT -> PREVIEW -> FUSION (lihat etl/dataset_manager.py:STAGE_TIER_INDEX). Kolom stage_order hanya urutan pendaftaran historis.';

INSERT INTO schema_migrations (version, description)
VALUES ('015', 'Add PREVIEW stage (PNG render dari GOLD, dijalankan sebelum FUSION)')
ON CONFLICT (version) DO NOTHING;

COMMIT;
