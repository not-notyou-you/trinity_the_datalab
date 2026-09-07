-- database/migrations/016_add_generate_preview_flag.sql
-- =============================================================================
-- datasets.generate_preview: opt-out per dataset untuk tahap PREVIEW
-- =============================================================================
--
-- Tahap PREVIEW (etl/module10_generate_preview.py, migrasi 015) me-render PNG
-- dari tier GOLD sebelum FUSION. Sebelum migrasi ini tahap itu selalu jalan
-- untuk setiap dataset yang mencapai GOLD; kolom ini membuatnya bisa dimatikan
-- per dataset lewat checkbox di form "Buat Dataset".
--
-- BERBEDA DARI MIGRASI 015 YANG OPSIONAL: migrasi ini WAJIB dijalankan sebelum
-- menjalankan kode versi ini. Model ORM `Dataset` di etl/database_client.py
-- memetakan kolom ini, jadi tanpa kolomnya SETIAP query ke tabel datasets akan
-- gagal (UndefinedColumn), bukan cuma jalur preview-nya.
--
-- Kenapa kolom sendiri, bukan key di dalam `quality_settings` JSONB:
--   * Ini pilihan user yang eksplisit dan bisa di-query/di-agregasi
--     ("berapa dataset yang preview-nya mati"), bukan ambang mutu.
--   * `quality_settings` isinya ambang kualitas data (min_cloud_cover,
--     min_quality_score, resolution_m). Menaruh sakelar rendering di sana
--     membuat nama kolomnya berbohong ke siapa pun yang membaca skema.
--   * DEFAULT TRUE + NOT NULL berarti semua dataset yang sudah ada otomatis
--     berperilaku persis seperti sebelumnya (PREVIEW ikut jalan), jadi tidak
--     ada perubahan perilaku yang tak diminta.
--
-- Kolom ini TIDAK ikut di `required_tiers`: PREVIEW bukan tier lineage (lihat
-- migrasi 015), jadi menambahkannya ke chk_required_tiers akan salah — dan
-- juga akan membuat compute_tiers_to_delete menghapus preview di setiap
-- dataset lama.

BEGIN;

ALTER TABLE datasets
    ADD COLUMN IF NOT EXISTS generate_preview BOOLEAN NOT NULL DEFAULT TRUE;

COMMENT ON COLUMN datasets.generate_preview IS
    'Jalankan tahap PREVIEW (render PNG dari GOLD, etl/module10_generate_preview.py) untuk dataset ini. FALSE mempercepat pipeline dengan melewatkan render. Tidak berpengaruh pada dataset yang berhenti sebelum GOLD -- di sana PREVIEW memang sudah dilewati.';

INSERT INTO schema_migrations (version, description)
VALUES ('016', 'Add datasets.generate_preview flag (opt-out tahap PREVIEW per dataset)')
ON CONFLICT (version) DO NOTHING;

COMMIT;
