-- database/migrations/022_widen_cpu_usage_percent.sql
-- =============================================================================
-- processing_jobs.cpu_usage_percent: NUMERIC(5,2) -> NUMERIC(7,2)
-- =============================================================================
--
-- KENAPA
-- psutil.Process.cpu_percent() menjumlahkan pemakaian SELURUH core, jadi
-- batasnya 100 persen x jumlah core, bukan 100 persen. Selama reproject/merge masih satu
-- thread, angkanya jarang lewat beberapa ratus persen dan NUMERIC(5,2)
-- (maksimum 999,99) cukup.
--
-- Setelah GDAL dan rasterio.warp dipakai dengan semua core (24 core di mesin
-- ini), puncaknya sampai 1944 persen dan setiap penulisan metrik gagal dengan
-- "numeric field overflow". Itu bukan sekadar metrik hilang: UPDATE yang sama
-- juga menulis status SUCCESS, jadi seluruh tahap scene ikut gagal. Di run
-- 2026-09-20 ini menggagalkan 9 tahap di dataset JAWA dan 5 di
-- jan_mar_2025_hybrid.
--
-- NUMERIC(7,2) menampung sampai 99.999,99 persen -- cukup untuk 999 core, jauh di
-- atas mesin mana pun yang realistis untuk pipeline ini.
--
-- Nilai lama tidak perlu diubah: melebarkan presisi tidak mengubah angka yang
-- sudah tersimpan, dan NUMERIC tanpa data di luar jangkauan tidak perlu
-- ditulis ulang.

BEGIN;

ALTER TABLE processing_jobs
    ALTER COLUMN cpu_usage_percent TYPE NUMERIC(7,2);

COMMIT;
