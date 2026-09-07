
-- Most common query: scenes filtered by region + date range + availability
CREATE INDEX IF NOT EXISTS idx_scenes_region_date
    ON satellite_scenes (region_id, acquisition_datetime DESC)
    WHERE is_available = TRUE;

-- Quality filter: scenes with GOLD products above quality threshold
CREATE INDEX IF NOT EXISTS idx_quality_score_flag
    ON quality_metrics (quality_score DESC, quality_flag, scene_id);

-- Pipeline monitoring: all jobs for a scene ordered by stage
CREATE INDEX IF NOT EXISTS idx_jobs_scene_stage
    ON processing_jobs (scene_id, stage_id, attempt_number DESC);

-- API logs: per-endpoint performance analysis
CREATE INDEX IF NOT EXISTS idx_apilogs_endpoint_time
    ON api_access_logs (endpoint, response_time_ms, request_timestamp DESC);

-- Data products: find latest GOLD product per scene+band (most common query)
CREATE INDEX IF NOT EXISTS idx_products_scene_band_tier
    ON data_products (scene_id, band_name, product_tier, created_at DESC)
    WHERE is_latest = TRUE AND is_valid = TRUE;

-- Lineage: fast forward/backward traversal
CREATE INDEX IF NOT EXISTS idx_lineage_transform_type
    ON data_lineage (transformation_type, created_at DESC);

-- Alert monitoring: unresolved critical alerts dashboard
CREATE INDEX IF NOT EXISTS idx_alerts_critical_unresolved
    ON alert_events (severity, triggered_at DESC)
    WHERE is_resolved = FALSE AND severity = 'CRITICAL';

-- Dataset versions: current production releases
CREATE INDEX IF NOT EXISTS idx_versions_production
    ON dataset_versions (release_date DESC)
    WHERE is_production = TRUE AND is_deprecated = FALSE;

-- ---------------------------------------------------------------------------
-- GIN index for JSONB columns (full-text search inside JSON)
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_jobs_params_gin
    ON processing_jobs USING GIN (parameters_json);

CREATE INDEX IF NOT EXISTS idx_lineage_params_gin
    ON data_lineage USING GIN (transformation_params);

CREATE INDEX IF NOT EXISTS idx_alerts_metadata_gin
    ON alert_events USING GIN (metadata_json);
