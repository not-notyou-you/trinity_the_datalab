-- database/migrations/017_add_dataset_source_config.sql
-- =============================================================================
-- Per-satellite processing model: dataset_source_config + kolom pendukungnya
-- =============================================================================
--
-- CATATAN NOMOR: brief menyebut file ini "016_add_dataset_source_config.sql",
-- tapi nomor 016 sudah dipakai 016_add_generate_preview_flag.sql yang sudah
-- ter-commit. Nomor migrasi adalah kunci unik di schema_migrations, jadi
-- memakai ulang 016 membuat migrasi ini di-skip diam-diam (ON CONFLICT DO
-- NOTHING) di database mana pun yang sudah menjalankan 016. Karena itu 017.
--
-- APA YANG BERUBAH
-- Sebelum ini konfigurasi pemrosesan sebuah dataset bersifat global: satu
-- `required_tiers` untuk semua sensor. Model baru (DOCS/ARCHITECTURE.md, bagian
-- "dataset_source_config") memberi SETIAP satelit definisi pemrosesannya
-- sendiri -- SENTINEL1 boleh PROCESSED sementara GPM cukup RAW.
--
-- Relasi dataset:sumber adalah 1:N dan tiap pasangan punya atributnya sendiri
-- (`processing_levels`), jadi bentuk yang benar adalah junction table, bukan
-- kolom array di `datasets`. Array di datasets tidak bisa membawa atribut
-- per-sumber tanpa berubah jadi JSON tanpa constraint.
--
-- BACKWARD COMPATIBILITY
--   * Backfill memasukkan 3 baris (SENTINEL1, MODIS, GPM) dengan
--     processing_levels = {PROCESSED} untuk SETIAP dataset yang sudah ada.
--     Itu persis perilaku prototipe: selalu tiga sumber, selalu jalur penuh.
--     Dataset lama karena itu berperilaku identik setelah migrasi.
--   * Semua kolom baru NULLable atau punya DEFAULT, jadi INSERT lama tetap
--     jalan tanpa perubahan.
--   * DROP COLUMN di bawah memakai IF EXISTS: di basis kode ini kolom
--     `selected_satellites` dan `datasets.processing_level` TIDAK PERNAH ADA
--     (lihat 004_add_dataset_management.sql), jadi DROP-nya no-op. Tetap
--     ditulis supaya database yang pernah ditambal manual ikut bersih.
--   * Seluruh migrasi idempoten: aman dijalankan ulang.
--
-- Definisi constraint di bawah sengaja diduplikasi di database/constraints.sql.
-- Migrasi harus berdiri sendiri (tidak bergantung pada `\i` psql yang
-- resolusi path-nya ikut cwd), sementara constraints.sql adalah berkas
-- verifikasi/perbaikan yang bisa dijalankan ulang kapan saja. Keduanya memakai
-- pola DROP-lalu-ADD yang sama sehingga hasil akhirnya identik.

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. Junction table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dataset_source_config (
    config_id          SERIAL       PRIMARY KEY,
    dataset_id         INTEGER      NOT NULL
                       REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    source_name        VARCHAR(20)  NOT NULL,
    processing_levels  TEXT[]       NOT NULL DEFAULT ARRAY['PROCESSED']::TEXT[],
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE dataset_source_config IS
    'Konfigurasi pemrosesan per-satelit untuk sebuah dataset. Satu baris per (dataset, sumber). ETL membaca tabel ini untuk menentukan sumber mana yang dijalankan dan sampai level apa (DOCS/PIPELINE.md).';
COMMENT ON COLUMN dataset_source_config.processing_levels IS
    'Level yang diminta untuk sumber ini: {RAW}, {PROCESSED}, atau {RAW,PROCESSED}. RAW berhenti di BRONZE; PROCESSED lanjut ke SILVER/GOLD. Arti per-satelit ada di DOCS/ARCHITECTURE.md.';

-- Constraint: satu baris konfigurasi per (dataset, sumber). UNIQUE ini juga
-- yang menopang ON CONFLICT di backfill, jadi harus ada sebelum INSERT.
ALTER TABLE dataset_source_config
    DROP CONSTRAINT IF EXISTS uq_source_config_dataset_source;
ALTER TABLE dataset_source_config
    ADD CONSTRAINT uq_source_config_dataset_source
    UNIQUE (dataset_id, source_name);

-- Constraint: minimal satu level. Array kosong berarti "sumber ini dipilih
-- tapi tidak diproses sama sekali" -- state yang tidak punya arti dan akan
-- membuat ETL menghasilkan dataset kosong tanpa error. array_length() atas
-- array kosong mengembalikan NULL, bukan 0, karena itu IS NOT NULL.
ALTER TABLE dataset_source_config
    DROP CONSTRAINT IF EXISTS chk_source_config_levels_not_empty;
ALTER TABLE dataset_source_config
    ADD CONSTRAINT chk_source_config_levels_not_empty CHECK (
        array_length(processing_levels, 1) IS NOT NULL
        AND array_length(processing_levels, 1) > 0
    );

-- Constraint: hanya nilai level yang dikenal pipeline.
ALTER TABLE dataset_source_config
    DROP CONSTRAINT IF EXISTS chk_source_config_levels_valid;
ALTER TABLE dataset_source_config
    ADD CONSTRAINT chk_source_config_levels_valid CHECK (
        processing_levels <@ ARRAY['RAW', 'PROCESSED']::TEXT[]
    );

-- Constraint: hanya sumber yang punya modul ETL.
ALTER TABLE dataset_source_config
    DROP CONSTRAINT IF EXISTS chk_source_config_source_name;
ALTER TABLE dataset_source_config
    ADD CONSTRAINT chk_source_config_source_name CHECK (
        source_name IN ('SENTINEL1', 'MODIS', 'GPM')
    );

CREATE INDEX IF NOT EXISTS idx_source_config_dataset
    ON dataset_source_config (dataset_id);

-- ---------------------------------------------------------------------------
-- 2. Backfill -- prototipe selalu menjalankan 3 sumber sebagai PROCESSED
-- ---------------------------------------------------------------------------
-- ON CONFLICT DO NOTHING membuat langkah ini aman diulang dan aman dijalankan
-- di database yang sebagian datasetnya sudah dikonfigurasi manual: baris yang
-- sudah ada tidak ditimpa.
INSERT INTO dataset_source_config (dataset_id, source_name, processing_levels)
SELECT d.dataset_id, s.source_name, ARRAY['PROCESSED']::TEXT[]
FROM   datasets d
CROSS JOIN (VALUES ('SENTINEL1'), ('MODIS'), ('GPM')) AS s(source_name)
ON CONFLICT (dataset_id, source_name) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3. Kolom baru di datasets
-- ---------------------------------------------------------------------------
ALTER TABLE datasets
    ADD COLUMN IF NOT EXISTS fusion_strategy VARCHAR(20) DEFAULT 'FULL_COVERAGE',
    ADD COLUMN IF NOT EXISTS preview_options TEXT[]
        DEFAULT ARRAY['GRAYSCALE', 'COLORED', 'COMPOSITE']::TEXT[];

-- NULL diizinkan: dataset satu-sumber tidak punya strategi fusi (DESIGN.md).
-- Aturan "harus NULL kalau sumbernya cuma 1" TIDAK ditegakkan di sini karena
-- jumlah sumber ada di tabel lain -- CHECK tidak boleh membaca tabel lain.
-- Penegakannya milik lapisan aplikasi (atau trigger, kalau nanti diperlukan).
ALTER TABLE datasets
    DROP CONSTRAINT IF EXISTS chk_datasets_fusion_strategy;
ALTER TABLE datasets
    ADD CONSTRAINT chk_datasets_fusion_strategy CHECK (
        fusion_strategy IS NULL
        OR fusion_strategy IN ('CO_OCCURRENCE', 'FULL_COVERAGE', 'HYBRID')
    );

COMMENT ON COLUMN datasets.fusion_strategy IS
    'Strategi pemasangan scene lintas sensor: CO_OCCURRENCE (hanya tanggal yang beririsan), FULL_COVERAGE (semua tanggal S1, sensor lain diambil terdekat), HYBRID. NULL = dataset satu sumber, tidak ada fusi.';
COMMENT ON COLUMN datasets.preview_options IS
    'Varian PNG yang dirender tahap PREVIEW. Array kosong = tidak ada varian. Sakelar on/off tahapnya tetap datasets.generate_preview (migrasi 016).';

-- Kolom model lama. Tidak ada di skema ini; DROP defensif untuk database yang
-- pernah ditambal manual. Dijalankan SETELAH backfill supaya, kalau kolomnya
-- memang ada, datanya masih terbaca sampai backfill selesai.
ALTER TABLE datasets
    DROP COLUMN IF EXISTS selected_satellites,
    DROP COLUMN IF EXISTS processing_level;

-- ---------------------------------------------------------------------------
-- 4. data_products.processing_level
-- ---------------------------------------------------------------------------
-- DEFAULT 'PROCESSED' menandai semua produk lama dengan benar: sebelum migrasi
-- ini pipeline hanya punya satu jalur, yaitu jalur penuh.
ALTER TABLE data_products
    ADD COLUMN IF NOT EXISTS processing_level VARCHAR(20) DEFAULT 'PROCESSED';

ALTER TABLE data_products
    DROP CONSTRAINT IF EXISTS chk_dprods_processing_level;
ALTER TABLE data_products
    ADD CONSTRAINT chk_dprods_processing_level CHECK (
        processing_level IS NULL OR processing_level IN ('RAW', 'PROCESSED')
    );

CREATE INDEX IF NOT EXISTS idx_dprods_dataset_level
    ON data_products (dataset_id, processing_level);

COMMENT ON COLUMN data_products.processing_level IS
    'Level pemrosesan yang menghasilkan artefak ini (RAW | PROCESSED). Berbeda dari product_tier: tier adalah posisi di lineage, processing_level adalah konfigurasi yang diminta user untuk sumbernya.';

-- ---------------------------------------------------------------------------
-- 5. Metadata fusi
-- ---------------------------------------------------------------------------
ALTER TABLE fusion_products
    ADD COLUMN IF NOT EXISTS fusion_strategy       VARCHAR(20) DEFAULT 'FULL_COVERAGE',
    ADD COLUMN IF NOT EXISTS processing_level      VARCHAR(20) DEFAULT 'PROCESSED',
    ADD COLUMN IF NOT EXISTS temporal_offset_modis INTEGER,
    ADD COLUMN IF NOT EXISTS temporal_offset_gpm   INTEGER;

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

-- Offset dibiarkan NULL untuk baris lama: nilainya tidak diketahui, dan 0 akan
-- berbohong bahwa MODIS/GPM diambil di hari yang sama dengan S1.
COMMENT ON COLUMN fusion_products.temporal_offset_modis IS
    'Selisih hari antara scene MODIS dan tanggal S1 pada stack ini. NULL = tidak diketahui (baris sebelum migrasi 017) atau MODIS tidak ikut.';
COMMENT ON COLUMN fusion_products.temporal_offset_gpm IS
    'Selisih hari antara scene GPM dan tanggal S1 pada stack ini. NULL = tidak diketahui (baris sebelum migrasi 017) atau GPM tidak ikut.';

INSERT INTO schema_migrations (version, description)
VALUES ('017', 'Per-satellite processing: dataset_source_config junction table + backfill, datasets.fusion_strategy/preview_options, data_products.processing_level, fusion_products fusion metadata')
ON CONFLICT (version) DO NOTHING;

COMMIT;

-- =============================================================================
-- KUERI VERIFIKASI (jalankan manual SETELAH migrasi; bukan bagian transaksi)
-- =============================================================================
-- Semuanya sudah dijalankan di database bersih `trinity_datalab_test` dengan
-- urutan: schema.sql -> migrasi 001..018. Hasil yang diharapkan ada di bawah
-- tiap kueri.
--
-- 1. Backfill: satu baris per (dataset, sumber) -> 3 x jumlah dataset.
--    SELECT (SELECT COUNT(*) FROM dataset_source_config) AS config_rows,
--           (SELECT COUNT(*) * 3 FROM datasets)          AS expected;
--    -> config_rows = expected
--
-- 2. Isi backfill = perilaku prototipe (3 sumber, selalu PROCESSED):
--    SELECT dataset_id, source_name, processing_levels
--    FROM   dataset_source_config ORDER BY dataset_id, source_name;
--    -> tiap dataset punya GPM/MODIS/SENTINEL1, semuanya {PROCESSED}
--
-- 3. UNIQUE(dataset_id, source_name) -- pakai dataset_id yang MEMANG ADA;
--    dataset_id yang tidak ada gagal lebih dulu di foreign key, jadi
--    UNIQUE-nya tidak pernah teruji:
--    INSERT INTO dataset_source_config (dataset_id, source_name, processing_levels)
--    VALUES (1, 'MODIS', ARRAY['RAW']);
--    -> ERROR: duplicate key ... "uq_source_config_dataset_source"
--
-- 4. Array kosong ditolak:
--    INSERT INTO dataset_source_config (dataset_id, source_name, processing_levels)
--    VALUES (1, 'GPM', '{}');
--    -> ERROR: ... "chk_source_config_levels_not_empty"
--
-- 5. Level di luar {RAW, PROCESSED} ditolak:
--    INSERT ... VALUES (1, 'GPM', ARRAY['GOLD']);
--    -> ERROR: ... "chk_source_config_levels_valid"
--
-- 6. Sumber tanpa modul ETL ditolak:
--    INSERT ... VALUES (1, 'LANDSAT', ARRAY['RAW']);
--    -> ERROR: ... "chk_source_config_source_name"
--
-- 7. Konfigurasi multi-level yang sah diterima:
--    INSERT INTO dataset_source_config (dataset_id, source_name, processing_levels)
--    VALUES (1, 'SENTINEL1', ARRAY['RAW', 'PROCESSED'])
--    ON CONFLICT (dataset_id, source_name)
--    DO UPDATE SET processing_levels = EXCLUDED.processing_levels;
--    -> INSERT 0 1
--
-- 8. Hapus dataset ikut menghapus konfigurasinya (ON DELETE CASCADE):
--    DELETE FROM datasets WHERE dataset_id = <id>;
--    SELECT COUNT(*) FROM dataset_source_config WHERE dataset_id = <id>;  -> 0
--
-- 9. Bentuk akhir tabel:  \d datasets   \d data_products
--                         \d fusion_products   \d dataset_source_config
--    -> datasets        : fusion_strategy, preview_options ada; TIDAK ada
--                         selected_satellites / processing_level
--    -> data_products   : processing_level DEFAULT 'PROCESSED'
--    -> fusion_products : fusion_strategy, processing_level,
--                         temporal_offset_modis, temporal_offset_gpm
--
-- 10. Idempotensi: jalankan ulang berkas ini -- harus sukses tanpa perubahan
--     (semua DDL pakai IF EXISTS/IF NOT EXISTS, backfill pakai ON CONFLICT).
