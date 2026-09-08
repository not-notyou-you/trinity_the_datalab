-- database/constraints.sql
-- =============================================================================
-- Constraint integritas untuk model pemrosesan per-satelit
-- =============================================================================
--
-- Berkas ini BUKAN migrasi. Isinya definisi constraint yang bisa dijalankan
-- ulang kapan saja untuk memverifikasi atau memulihkan aturan integritas di
-- database yang sudah ada -- misalnya setelah restore dump lama, atau setelah
-- seseorang men-drop constraint secara manual saat debugging.
--
-- Definisi di sini SENGAJA identik dengan yang ada di migrasi
-- 017_add_dataset_source_config.sql. Migrasi harus berdiri sendiri (tidak
-- boleh bergantung pada `\i` psql yang resolusi path-nya ikut cwd, dan tidak
-- boleh berubah isinya setelah dijalankan orang lain), sedangkan berkas ini
-- adalah alat perawatan. Keduanya memakai pola DROP-lalu-ADD sehingga
-- menjalankan salah satu, atau keduanya, memberi hasil akhir yang sama.
--
-- Cara pakai:
--     psql "$DATABASE_URL" -f database/constraints.sql
--
-- Menjalankan ini pada tabel yang datanya melanggar aturan akan GAGAL dengan
-- error -- itu memang tujuannya: berkas ini juga berfungsi sebagai pemeriksa
-- integritas, bukan cuma pemasang constraint.

BEGIN;

-- ---------------------------------------------------------------------------
-- dataset_source_config
-- ---------------------------------------------------------------------------

-- Satu baris konfigurasi per (dataset, sumber). Duplikat berarti pipeline
-- punya dua definisi pemrosesan yang berbeda untuk satelit yang sama pada satu
-- dataset, dan mana yang menang tergantung urutan baris -- non-deterministik.
-- UNIQUE ini juga yang menopang klausa ON CONFLICT di backfill migrasi 017.
ALTER TABLE dataset_source_config
    DROP CONSTRAINT IF EXISTS uq_source_config_dataset_source;
ALTER TABLE dataset_source_config
    ADD CONSTRAINT uq_source_config_dataset_source
    UNIQUE (dataset_id, source_name);

-- processing_levels tidak boleh array kosong. Array kosong berarti "sumber ini
-- dipilih tapi tidak diproses sama sekali" -- state yang tidak punya arti dan
-- akan membuat ETL menghasilkan dataset kosong tanpa memunculkan error.
--
-- array_length(x, 1) atas array kosong mengembalikan NULL, bukan 0, dan CHECK
-- meloloskan NULL. Karena itu cek IS NOT NULL-nya wajib; `> 0` sendirian tidak
-- akan menangkap '{}'.
ALTER TABLE dataset_source_config
    DROP CONSTRAINT IF EXISTS chk_source_config_levels_not_empty;
ALTER TABLE dataset_source_config
    ADD CONSTRAINT chk_source_config_levels_not_empty CHECK (
        array_length(processing_levels, 1) IS NOT NULL
        AND array_length(processing_levels, 1) > 0
    );

-- Hanya level yang dikenal pipeline. `<@` = "terkandung dalam", jadi ini
-- meloloskan {RAW}, {PROCESSED}, dan {RAW,PROCESSED}, tapi menolak salah ketik
-- seperti {PROCCESSED} yang kalau lolos akan bikin sumbernya dilewati diam-diam.
ALTER TABLE dataset_source_config
    DROP CONSTRAINT IF EXISTS chk_source_config_levels_valid;
ALTER TABLE dataset_source_config
    ADD CONSTRAINT chk_source_config_levels_valid CHECK (
        processing_levels <@ ARRAY['RAW', 'PROCESSED']::TEXT[]
    );

-- Hanya sumber yang punya modul ETL.
ALTER TABLE dataset_source_config
    DROP CONSTRAINT IF EXISTS chk_source_config_source_name;
ALTER TABLE dataset_source_config
    ADD CONSTRAINT chk_source_config_source_name CHECK (
        source_name IN ('SENTINEL1', 'MODIS', 'GPM')
    );

-- ---------------------------------------------------------------------------
-- datasets
-- ---------------------------------------------------------------------------

-- NULL diizinkan: dataset satu-sumber tidak punya strategi fusi (DESIGN.md).
--
-- Aturan "fusion_strategy harus NULL kalau sumbernya cuma 1" TIDAK bisa
-- ditegakkan sebagai CHECK: jumlah sumber ada di dataset_source_config, dan
-- CHECK tidak boleh membaca tabel lain (Postgres tidak menegakkannya ulang
-- saat tabel lain berubah, jadi hasilnya constraint yang bisa jadi salah tanpa
-- ketahuan). Penegakannya milik lapisan aplikasi.
ALTER TABLE datasets
    DROP CONSTRAINT IF EXISTS chk_datasets_fusion_strategy;
ALTER TABLE datasets
    ADD CONSTRAINT chk_datasets_fusion_strategy CHECK (
        fusion_strategy IS NULL
        OR fusion_strategy IN ('CO_OCCURRENCE', 'FULL_COVERAGE', 'HYBRID')
    );

-- ---------------------------------------------------------------------------
-- data_products / fusion_products
-- ---------------------------------------------------------------------------

-- NULL diloloskan supaya baris pra-017 yang belum ditandai tidak membuat
-- pemasangan constraint ini gagal di database lama.
ALTER TABLE data_products
    DROP CONSTRAINT IF EXISTS chk_dprods_processing_level;
ALTER TABLE data_products
    ADD CONSTRAINT chk_dprods_processing_level CHECK (
        processing_level IS NULL OR processing_level IN ('RAW', 'PROCESSED')
    );

ALTER TABLE fusion_products
    DROP CONSTRAINT IF EXISTS chk_fusion_processing_level;
ALTER TABLE fusion_products
    ADD CONSTRAINT chk_fusion_processing_level CHECK (
        processing_level IS NULL OR processing_level IN ('RAW', 'PROCESSED')
    );

ALTER TABLE fusion_products
    DROP CONSTRAINT IF EXISTS chk_fusion_strategy;
ALTER TABLE fusion_products
    ADD CONSTRAINT chk_fusion_strategy CHECK (
        fusion_strategy IS NULL
        OR fusion_strategy IN ('CO_OCCURRENCE', 'FULL_COVERAGE', 'HYBRID')
    );

-- Kunci unik fusion_products memuat processing_level (migrasi 018): dataset
-- yang meminta sebuah sumber di RAW dan PROCESSED sekaligus menghasilkan DUA
-- stack untuk tanggal yang sama. Kunci lama (feature_date, region_id) membuat
-- yang kedua menimpa yang pertama.
--
-- Kolomnya di-backfill dulu: di PostgreSQL NULL tidak pernah sama dengan NULL,
-- jadi beberapa baris ber-processing_level NULL bisa berdampingan dan kunci
-- uniknya tidak menahan apa pun.
UPDATE fusion_products SET processing_level = 'PROCESSED'
WHERE  processing_level IS NULL;

ALTER TABLE fusion_products
    DROP CONSTRAINT IF EXISTS uq_fusion_date_region;
ALTER TABLE fusion_products
    DROP CONSTRAINT IF EXISTS uq_fusion_date_region_level;
ALTER TABLE fusion_products
    ADD CONSTRAINT uq_fusion_date_region_level
    UNIQUE (feature_date, region_id, processing_level);

COMMIT;
