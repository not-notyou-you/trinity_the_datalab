-- database/migrations/014_fix_required_tiers_fusion.sql
--
-- Migrasi 013 menambahkan tier FUSION ke product_tier_enum (dan schema.sql
-- ikut diperbarui), tapi `datasets.chk_required_tiers` dari migrasi 004 masih
-- memakai whitelist lama:
--
--     required_tiers <@ ARRAY['RAW', 'BRONZE', 'SILVER', 'GOLD']
--
-- Akibatnya setiap INSERT dataset STANDARD dari UI (checkbox FUSION default
-- tercentang -- web/index.html, web/app.js TIER_ORDER) ditolak:
--
--     CheckViolation: new row for relation "datasets" violates check
--     constraint "chk_required_tiers"
--     DETAIL: ... required_tiers: {RAW,BRONZE,SILVER,GOLD,FUSION}
--
-- Dataset LIVE tidak pernah kena karena baris seed-nya (migrasi 004) memakai
-- ARRAY['GOLD'] saja dan tidak pernah di-INSERT ulang.
--
-- FUSION bukan tier khusus LIVE: module5_orchestrator.run_dataset_job ->
-- _stage_fusion berjalan untuk dataset STANDARD juga, dijalankan/dilewati
-- lewat compute_skip_stages(required_tiers) (etl/dataset_manager.py
-- STAGE_TIER_INDEX["FUSION"] = 4). Jadi yang salah adalah constraint-nya,
-- bukan daftar tier yang dikirim aplikasi.

ALTER TABLE datasets DROP CONSTRAINT IF EXISTS chk_required_tiers;
ALTER TABLE datasets ADD CONSTRAINT chk_required_tiers CHECK (
    required_tiers <@ ARRAY['RAW', 'BRONZE', 'SILVER', 'GOLD', 'FUSION']::TEXT[]
    AND array_length(required_tiers, 1) > 0
);

COMMENT ON COLUMN datasets.required_tiers IS
    'Tier yang harus disimpan untuk dataset ini: RAW | BRONZE | SILVER | GOLD | FUSION. Urutan tier ada di etl/dataset_manager.TIER_ORDER; tier di atas tier tertinggi yang diminta dilewati (compute_skip_stages), tier yang terlanjur diproduksi tapi tidak diminta dihapus di tahap CLEANUP (compute_tiers_to_delete).';

INSERT INTO schema_migrations (version, description)
VALUES ('014', 'Allow FUSION in datasets.required_tiers: chk_required_tiers still had the pre-013 RAW/BRONZE/SILVER/GOLD whitelist')
ON CONFLICT (version) DO NOTHING;
