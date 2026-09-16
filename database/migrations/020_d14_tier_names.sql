-- database/migrations/020_d14_tier_names.sql
-- =============================================================================
-- D14: tier dinamai per kontrak, bukan medallion
-- =============================================================================
--
-- APA YANG BERUBAH
-- `product_tier_enum` mendapat enam nilai baru. Nama lama TIDAK dihapus dan
-- baris lama TIDAK diubah -- lihat "BACKWARD COMPATIBILITY" di bawah.
--
--     RAW      -> RAW                                   (tidak berubah)
--     BRONZE   -> ALIGNED                               EPSG:4326 + crop AOI
--     SILVER   -> DESPECKLED | INDICES | ACCUMULATED    per-source
--     GOLD     -> COG                                   analysis-ready
--     FUSION   -> FUSED                                 HDF5 multi-modal
--
-- MENGAPA
-- GOLD bukan mutu lebih tinggi dari SILVER: pikselnya identik, hanya dibungkus
-- ulang jadi COG -- itu keputusan FORMAT, bukan kualitas. Dan SILVER adalah
-- satu nama untuk tiga operasi yang tidak sejenis (Lee filter memperbaiki
-- variabel yang sama, NDVI/NDWI menciptakan variabel baru, akumulasi membuat
-- agregat temporal baru), yang merupakan konsekuensi langsung dari model
-- per-satelit di migrasi 017. Lihat DOCS/DECISIONS.md D14 dan D15.
--
-- Rank 1 (ALIGNED) dan rank 3 (COG) sengaja TIDAK dipecah per-source: ketiga
-- satelit menjamin hal yang identik di sana.
--
-- CATATAN EKSEKUSI
-- `ALTER TYPE ... ADD VALUE` tidak bisa jalan di dalam blok transaksi pada
-- PostgreSQL < 12, dan bahkan di versi baru nilainya tidak bisa dipakai di
-- transaksi yang sama. Karena itu berkas ini TIDAK dibungkus BEGIN/COMMIT --
-- jalankan apa adanya, jangan dengan --single-transaction:
--
--     psql "$DATABASE_URL" -f database/migrations/020_d14_tier_names.sql
--
-- BACKWARD COMPATIBILITY
-- Tidak ada UPDATE. Baris data_products yang ditulis sebelum migrasi ini tetap
-- bernilai BRONZE/SILVER/GOLD/FUSION, dan itu disengaja: tier lama menggambarkan
-- artefak yang memang dihasilkan aturan lama, dan menuliskan ulang namanya akan
-- mengklaim artefak itu memenuhi kontrak yang belum tentu dipenuhinya --
-- khususnya SILVER, yang tanpa kolom `source` tidak bisa dipetakan ke satu nama
-- rank 2 mana pun. Pembacaan ditangani etl/tier_names.py: rank() menerima kedua
-- kosakata, dan tiap filter `= 'GOLD'` sudah diganti `IN (...)` atas rank yang
-- sama.

ALTER TYPE product_tier_enum ADD VALUE IF NOT EXISTS 'ALIGNED'     AFTER 'RAW';
ALTER TYPE product_tier_enum ADD VALUE IF NOT EXISTS 'DESPECKLED'  AFTER 'ALIGNED';
ALTER TYPE product_tier_enum ADD VALUE IF NOT EXISTS 'INDICES'     AFTER 'DESPECKLED';
ALTER TYPE product_tier_enum ADD VALUE IF NOT EXISTS 'ACCUMULATED' AFTER 'INDICES';
ALTER TYPE product_tier_enum ADD VALUE IF NOT EXISTS 'COG'         AFTER 'ACCUMULATED';
ALTER TYPE product_tier_enum ADD VALUE IF NOT EXISTS 'FUSED'       AFTER 'COG';

-- View yang menyaring tier deliverable harus menjaring rank 3 di KEDUA
-- kosakata, karena baris lama sengaja tidak diubah.
-- (definisi lengkapnya ada di database/schema.sql; jalankan berkas itu ulang
--  atau salin CREATE OR REPLACE VIEW-nya ke sini kalau view-nya sudah ada)

-- datasets.chk_required_tiers (migrasi 004/014) masih whitelist kosakata lama,
-- sehingga INSERT dataset dari UI dengan nama tier D14 ditolak (CheckViolation).
-- Kedua kosakata diterima: baris lama tetap valid.
ALTER TABLE datasets DROP CONSTRAINT IF EXISTS chk_required_tiers;
ALTER TABLE datasets ADD CONSTRAINT chk_required_tiers CHECK (
    required_tiers <@ ARRAY[
        'RAW', 'ALIGNED', 'DESPECKLED', 'INDICES', 'ACCUMULATED', 'COG', 'FUSED',
        'BRONZE', 'SILVER', 'GOLD', 'FUSION'
    ]::TEXT[]
    AND array_length(required_tiers, 1) > 0
);

-- View deliverable: saring rank 3 di kedua kosakata (salinan dari schema.sql).
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
                             -- Rank 3 di kedua kosakata: baris pra-D14 tetap
                             -- terjaring (migrasi 020 tidak mengubah baris).
                             AND dp.product_tier IN ('COG', 'GOLD')
LEFT JOIN quality_metrics qm ON qm.scene_id   = ss.scene_id
                             AND qm.product_id = dp.product_id
WHERE ss.is_available = TRUE
ORDER BY ss.acquisition_datetime DESC;
