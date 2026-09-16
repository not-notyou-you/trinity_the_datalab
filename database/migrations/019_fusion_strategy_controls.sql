-- database/migrations/019_fusion_strategy_controls.sql
-- =============================================================================
-- Kontrol strategi fusi: toleransi pasangan S1, offset yang terpakai, dan
-- opsi "hanya simpan output fusi"
-- =============================================================================
--
-- APA YANG BERUBAH
-- Sampai migrasi 018, `datasets.fusion_strategy` sudah ada tapi tidak pernah
-- jadi percabangan: ia cuma dicatat ke atribut HDF5 dan ke
-- `fusion_products.fusion_strategy`. Seluruh pipeline berjangkar pada scene
-- Sentinel-1, sehingga ketiga strategi menghasilkan berkas yang identik.
--
-- Dengan etl/fusion_strategies.py, strategi terurai jadi dua sumbu -- tanggal
-- MODIS/GPM mana yang DIUNDUH, dan tanggal mana yang DIRAKIT jadi HDF5.
-- Dua sumbu itu memunculkan tiga hal yang perlu tempat di skema:
--
--   1. datasets.s1_match_tolerance_days
--      FULL_COVERAGE merakit satu berkas per hari, termasuk hari yang tidak
--      punya scene S1, jadi ia harus tahu seberapa jauh boleh meminjam scene
--      dari hari lain. Sentinel-1A sendirian revisit ~12 hari di ekuator,
--      sehingga memaksa same-day akan membuat hampir semua hari kehilangan S1.
--      Default 2 hari = DEFAULT_S1_MATCH_TOLERANCE_DAYS di kode.
--
--   2. fusion_products.s1_offset_days
--      Jarak hari yang BENAR-BENAR terpakai antara tanggal fusi dan scene S1
--      yang dipinjam. Tanpa kolom ini, fusi same-day dan fusi bertoleransi
--      tidak bisa dibedakan lagi setelah berkasnya ditulis -- padahal untuk
--      training keduanya sangat berbeda kualitasnya. NULL berarti hari itu
--      tidak punya S1 sama sekali (hanya mungkin di FULL_COVERAGE, yang tetap
--      menulis berkas dengan group sentinel1/ berisi NaN).
--      Sengaja TIDAK diisi mundur untuk baris lama: baris pra-019 semuanya
--      berjangkar S1 same-day, tapi menuliskan 0 di sana akan mengarang data
--      yang tidak pernah diukur. NULL yang jujur lebih berguna.
--
--   3. datasets.fusion_output_only
--      User yang cuma butuh HDF5 tidak perlu menyimpan artefak per-satelit.
--      Ini BUKAN "lewati pemrosesan" -- fusi tetap butuh bahannya; artefak
--      per-satelit dihapus SETELAH stack tanggal itu sukses ditulis.
--
-- BACKWARD COMPATIBILITY
-- Ketiganya aditif dengan default yang mempertahankan perilaku lama:
-- toleransi 2 hari hanya berlaku bagi FULL_COVERAGE (dua strategi lain
-- berjangkar S1 dan mengabaikannya), dan fusion_output_only=FALSE berarti
-- tidak ada yang dihapus. Tidak ada baris yang diubah.

BEGIN;

ALTER TABLE datasets
    ADD COLUMN IF NOT EXISTS fusion_output_only      BOOLEAN  NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS s1_match_tolerance_days SMALLINT NOT NULL DEFAULT 2;

-- Batas atas 14 hari: melebihi satu siklus revisit penuh Sentinel-1A (~12
-- hari) berarti "scene terdekat" sudah bisa berasal dari lintasan yang sama
-- sekali berbeda kondisinya, dan menyebutnya fusi akan menyesatkan.
ALTER TABLE datasets
    DROP CONSTRAINT IF EXISTS chk_datasets_s1_tolerance;
ALTER TABLE datasets
    ADD CONSTRAINT chk_datasets_s1_tolerance
    CHECK (s1_match_tolerance_days BETWEEN 0 AND 14);

ALTER TABLE fusion_products
    ADD COLUMN IF NOT EXISTS s1_offset_days SMALLINT;

COMMENT ON COLUMN datasets.fusion_output_only IS
    'Hapus artefak per-satelit setelah stack fusi tanggal itu ditulis.';
COMMENT ON COLUMN datasets.s1_match_tolerance_days IS
    'FULL_COVERAGE: jarak hari maksimum untuk meminjam scene S1. 0 = same-day saja.';
COMMENT ON COLUMN fusion_products.s1_offset_days IS
    'Jarak hari S1 yang terpakai. 0 = same-day, NULL = tanpa S1 (group NaN).';

COMMIT;
