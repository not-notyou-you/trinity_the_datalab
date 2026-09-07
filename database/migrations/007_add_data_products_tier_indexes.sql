-- database/migrations/007_add_data_products_tier_indexes.sql
--
-- data_products.dataset_id and data_products.scene_id are foreign keys but
-- Postgres does not index them automatically. With the gold/silver/bronze/raw
-- folder restructure (data/datasets/{dataset_id}/{acquisition_date}/{tier}/),
-- storage-summary and per-tier file listing queries filter by
-- (dataset_id, product_tier) and join through scene_id to get
-- satellite_scenes.acquisition_datetime, so both need an index to stay fast
-- as datasets grow.

CREATE INDEX IF NOT EXISTS idx_data_products_dataset_tier
    ON data_products (dataset_id, product_tier);

CREATE INDEX IF NOT EXISTS idx_data_products_scene_id
    ON data_products (scene_id);

INSERT INTO schema_migrations (version, description)
VALUES ('007', 'Add indexes on data_products(dataset_id, product_tier) and data_products(scene_id) for per-dataset/per-tier storage queries')
ON CONFLICT (version) DO NOTHING;
