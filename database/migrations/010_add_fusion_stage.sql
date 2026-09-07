-- database/migrations/010_add_fusion_stage.sql
--
-- GOLD tier is being refactored to mean "HDF5 multi-modal fusion stack"
-- (etl/module9_fusion.py) instead of per-band Cloud-Optimized GeoTIFFs
-- (etl/module4_cog_export.py, now removed). Add the FUSION processing
-- stage so module5_orchestrator.py can register FUSION processing_jobs.
--
-- The COG_EXPORT row is left in place (not deleted, not renamed): historical
-- processing_jobs/data_lineage rows still reference it via FK, and dropping
-- it would break those. New jobs simply stop being created against it.

INSERT INTO processing_stages (stage_name, stage_code, stage_order, description, timeout_minutes, retry_count, retry_delay_sec)
VALUES
    ('FUSION', 'FS', 7, 'Multi-modal HDF5 feature stack fusion (Sentinel-1 + MODIS + GPM) for GOLD tier', 60, 2, 30)
ON CONFLICT (stage_name) DO NOTHING;

INSERT INTO schema_migrations (version, description)
VALUES ('010', 'Add FUSION processing stage: GOLD tier is now the HDF5 fusion stack, not per-band COGs')
ON CONFLICT (version) DO NOTHING;
