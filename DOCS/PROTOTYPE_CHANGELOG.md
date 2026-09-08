# Prototype → DataLab Changelog

This document describes what changed conceptually and architecturally from the earlier Trinity Prototype to Trinity: The DataLab. The DataLab is developed as a new product; this changelog exists for developer context only.

## Core Conceptual Shift

**Prototype**: Fixed pipeline. Hardcoded to ingest all three satellites, always run full processing, always produce fusion. No user choice over processing or fusion behavior.

**DataLab**: Parametric pipeline. Users configure satellites, processing level, fusion strategy, and preview options at dataset creation time. The pipeline branches based on these parameters.

## New: Per-Satellite Processing Configuration

**Prototype**: All three satellites always ran with fixed processing. No user choice.

**DataLab**: Each satellite has its own RAW/PROCESSED toggle, configured independently via a new `dataset_source_config` table:

| Source | RAW (skip) | PROCESSED (full) |
|---|---|---|
| Sentinel-1 | Calibrate + crop only (no Lee filter, no QA) | + Lee filter + QA + COG |
| MODIS | Flood map only (no derived indices) | + NDVI + NDWI computation |
| GPM | Daily rainfall only | + 24h/72h/7d accumulation |

The old single `processing_level TEXT[]` column on `datasets` is replaced by a junction table. The `data_products` table gains a `processing_level` column to tag each artifact.

## New: Fusion Strategy Selection

**Prototype**: Fusion always used full-coverage (fuse every S1 date, fill missing MODIS/GPM with ±1 day search, NaN if unavailable).

**DataLab**: Three strategies available:
- `CO_OCCURRENCE`: Only fuse dates where all selected sources have same-day data
- `FULL_COVERAGE`: Fuse every date, fill missing with ±1-2 day or NaN
- `HYBRID`: Daily auxiliary, S1-anchored fusion dates

The `fusion_products` table gains `fusion_strategy` and explicit `temporal_offset_*` columns.

## New: Selective Satellite Ingestion

**Prototype**: Always ingested Sentinel-1 + MODIS + GPM. All three pipelines always ran.

**DataLab**: Users select 1–3 sources, each with its own processing level. Pipeline only runs modules for configured sources. Fusion disabled when only 1 source configured. API `sources` object replaces the old flat `selected_satellites` array.

## New: Configurable Preview Options

**Prototype**: Previews were all-or-nothing (controlled by `generate_preview: true/false`).

**DataLab**: Users select specific preview types: GRAYSCALE, COLORED, COMPOSITE, or any combination. Empty selection = no previews.

## Changed: Dataset Creation API

**Prototype request**:
```json
{
  "location": "Jabodetabek",
  "date_start": "2024-01-01",
  "date_end": "2024-01-31",
  "tiers": ["GOLD", "FUSION"],
  "name": "..."
}
```

**DataLab request**:
```json
{
  "location": "Jabodetabek",
  "date_start": "2024-01-01",
  "date_end": "2024-01-31",
  "name": "...",
  "sources": {
    "sentinel1": { "processing": ["RAW", "PROCESSED"] },
    "modis": { "processing": ["PROCESSED"] },
    "gpm": { "processing": ["RAW"] }
  },
  "fusion_strategy": "CO_OCCURRENCE",
  "preview_options": ["COLORED"]
}
```

`tiers` is no longer user-facing — derived internally. `sources` replaces both `selected_satellites` and `processing_level`.

## Changed: Products API

New filter parameter: `GET /api/products?processing_level=RAW` to retrieve only RAW-level or PROCESSED-level artifacts.

## Changed: Pipeline Orchestrator

**Prototype**: Linear stage sequence, always all stages, same for all sources.

**DataLab**: Per-source conditional branching:
- S1: Stages 4–6 (Lee filter, QA, COG) skipped if S1 config is RAW-only
- MODIS: NDVI/NDWI computation skipped if MODIS config is RAW-only
- GPM: Accumulation windows skipped if GPM config is RAW-only
- Fusion skipped if single source configured
- Fusion strategy selector determines date matching logic
- Preview stage branches on selected preview types

## Changed: Web Dashboard (Tab 1: Buat Dataset)

**Prototype**: Simple form — location, dates, tier checkboxes, create.

**DataLab**: 4-step wizard:
1. Region & Date (same)
2. Satellite & Processing Selection (new — per-satellite cards with individual RAW/PROCESSED toggles + "Pilih Semua" master toggle)
3. Fusion & Preview (new — fusion strategy radio, preview option checkboxes)
4. Review & Create (new — summary before submission)

## Changed: Dataset Cards (Tab 2)

Now display the dataset's configuration: selected satellites, processing level, fusion strategy. Previously only showed name/region/dates/progress.

## Unchanged

These components carry over architecturally (reimplemented, not forked):
- 6-tier lakehouse storage layout
- Sentinel-1 download + calibrate + crop + Lee filter pipeline
- MODIS download + NDVI/NDWI computation
- GPM download + accumulation windows
- SHA-256 lineage tracking
- PostgreSQL + PostGIS + TimescaleDB schema foundation
- APScheduler live ingestion
- Pause/resume/cancel via threading.Event
- Quality scoring formula
- FastAPI REST API structure
- Vanilla JS + Leaflet frontend architecture

## Changed: Fusion HDF5 Output

**Prototype**: One HDF5 per date with a fixed set of eight layers — all three
sensors, always, NaN-filled when a sensor had no data.

**DataLab**: The group/layer names carry over unchanged
(`/sentinel1/VV`, `/modis/FLOOD`, `/gpm/rainfall_24h`, …), but *which* of them
appear is now derived from `dataset_source_config`:

- An unconfigured source contributes no group at all (a configured source with
  no data that day still gets its group, NaN-filled — the distinction matters
  to a consumer).
- A source configured RAW contributes only its raw artifact: `/modis/FLOOD`
  alone, or `/gpm/rainfall_daily` (a new layer name — one day of rainfall, no
  accumulation, so it is deliberately not called `rainfall_24h`).
- A dataset requesting any source at both levels produces **two** stacks per
  date instead of one.

Filenames gain the level: `fusion_{YYYYMMDD}_{raw|processed}.h5` with a matching
`fusion_metadata_{level}.json`. `data_products.band_name` for a fusion row is
`FUSION_RAW` / `FUSION_PROCESSED` rather than `FUSION`.

## Changed: Preview Output

**Prototype**: `preview/{date}/{grayscale,colored}/`, always rendered from GOLD,
with the RGB composite filed under `colored/`.

**DataLab**: `preview/{date}/{RAW|PROCESSED}/{grayscale,colored,composite}/`.
The level chooses the input tier (GOLD for PROCESSED, BRONZE for RAW) and
separates the two renders, which would otherwise overwrite each other's identically
named PNGs. The composite gets its own folder because its sidecar describes a
channel mapping, not a colormap.

## Database Migration Path

If migrating from Prototype DB to DataLab DB:

```sql
-- New junction table for per-satellite processing config
CREATE TABLE dataset_source_config (
  config_id SERIAL PRIMARY KEY,
  dataset_id INT NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
  source_name VARCHAR(20) NOT NULL,
  processing_levels TEXT[] NOT NULL DEFAULT '{"PROCESSED"}',
  UNIQUE(dataset_id, source_name)
);

-- Backfill: Prototype always ran all 3 sources as PROCESSED
INSERT INTO dataset_source_config (dataset_id, source_name, processing_levels)
SELECT dataset_id, unnest(ARRAY['SENTINEL1','MODIS','GPM']), '{"PROCESSED"}'
FROM datasets;

-- New columns on datasets
ALTER TABLE datasets ADD COLUMN fusion_strategy VARCHAR(20) DEFAULT 'FULL_COVERAGE';
ALTER TABLE datasets ADD COLUMN preview_options TEXT[] DEFAULT '{"GRAYSCALE","COLORED","COMPOSITE"}';

-- Tag existing products
ALTER TABLE data_products ADD COLUMN processing_level VARCHAR(20) DEFAULT 'PROCESSED';

-- Fusion metadata
ALTER TABLE fusion_products ADD COLUMN fusion_strategy VARCHAR(20) DEFAULT 'FULL_COVERAGE';
ALTER TABLE fusion_products ADD COLUMN processing_level VARCHAR(20) DEFAULT 'PROCESSED';
ALTER TABLE fusion_products ADD COLUMN temporal_offset_modis INT;
ALTER TABLE fusion_products ADD COLUMN temporal_offset_gpm INT;
```

Migration 018 then completes the per-level model (see
`database/migrations/018_fusion_per_processing_level.sql`):

```sql
-- Two stacks per date must be able to coexist
ALTER TABLE fusion_products DROP CONSTRAINT uq_fusion_date_region;
ALTER TABLE fusion_products
  ADD CONSTRAINT uq_fusion_date_region_level
  UNIQUE (feature_date, region_id, processing_level);

-- Room for 'FUSION_PROCESSED'
ALTER TABLE data_products ALTER COLUMN band_name TYPE VARCHAR(20);

-- Empty array must unambiguously mean "no previews"
UPDATE datasets SET preview_options = ARRAY['GRAYSCALE','COLORED','COMPOSITE']
WHERE preview_options IS NULL;
ALTER TABLE datasets ALTER COLUMN preview_options SET NOT NULL;
```
