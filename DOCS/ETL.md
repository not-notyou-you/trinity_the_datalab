# ETL Pipeline

## Pipeline Branching Logic

Each satellite has its **own processing definition**. The pipeline reads `dataset_source_config` to determine which sources to run and what processing level each one needs:

```
For each source in dataset_source_config:
  Run source-specific pipeline
  If "RAW" in source.processing_levels:
    Export RAW-level artifacts (source-specific minimal processing)
  If "PROCESSED" in source.processing_levels:
    Run full source-specific processing chain

If count(configured sources) > 1 AND fusion_strategy is set:
  Run fusion stage using configured strategy
```

Both RAW and PROCESSED artifacts coexist on disk when both are selected for a source. Each `data_products` row carries a `processing_level` tag.

## Output Folder Structure

```
data/datasets/{dataset_id}_{slug}/
│
├── metadata.json
│
├── sentinel1/                           ← Source folder (only if S1 configured)
│   ├── raw/                             ← Processing level (only if S1[RAW] configured)
│   │   ├── bronze/
│   │   │   └── {product_id}_{date}/
│   │   │       ├── VV.tif, VH.tif
│   │   │       └── metadata.json
│   │   └── preview/
│   │       └── {date}/
│   │           ├── grayscale/
│   │           │   ├── VV_p2_p98.png, VH_p2_p98.png
│   │           └── colored/
│   │               ├── VV_viridis.png, VH_viridis.png
│   │
│   └── processed/                      ← Processing level (only if S1[PROCESSED] configured)
│       ├── bronze/
│       │   └── {product_id}_{date}/
│       │       └── VV.tif, VH.tif
│       ├── silver/
│       │   └── {product_id}_{date}/
│       │       ├── VV_lee.tif, VH_lee.tif
│       │       └── metadata_qa.json (quality scores)
│       ├── gold/
│       │   └── {product_id}_{date}/
│       │       └── VV.cog, VH.cog (512×512 tiles, overviews)
│       └── preview/
│           └── {date}/
│               ├── grayscale/
│               ├── colored/
│               └── composite/
│
├── modis/                               ← Source folder (only if MODIS configured)
│   └── processed/                       ← (no RAW subfolder if only PROCESSED selected)
│       ├── bronze/
│       │   └── {tile}_{date}/
│       │       ├── FLOOD.tif, NDVI.tif, NDWI.tif
│       ├── silver/
│       │   └── {tile}_{date}/
│       │       ├── FLOOD.tif, NDVI.tif, NDWI.tif
│       ├── gold/
│       │   └── {tile}_{date}/
│       │       └── *.cog
│       └── preview/
│           └── {date}/
│               ├── grayscale/
│               └── colored/
│
├── gpm/                                 ← Source folder (only if GPM configured)
│   └── raw/                             ← Processing level (only if GPM[RAW] configured)
│       ├── bronze/
│       │   └── {date}/
│       │       └── rainfall_daily.tif
│       └── preview/ (typically empty for rain data)
│
│   ├── processed/                       ← Processing level (only if GPM[PROCESSED] configured)
│       ├── silver/
│       │   └── {date}/
│       │       ├── rainfall_24h.tif, rainfall_72h.tif, rainfall_7d.tif
│       ├── gold/
│       │   └── {date}/
│       │       └── *.cog
│       └── preview/
│           └── {date}/
│
└── fusion/                              ← Fusion stage output (only if >1 source + fusion_strategy set)
    ├── raw/                             ← Fusion using RAW-tier inputs (if any source has RAW configured)
    │   └── {date}/
    │       ├── fusion_{date}_raw.h5
    │       │   ├── /sentinel1/VV, /sentinel1/VH (if S1 configured)
    │       │   ├── /modis/FLOOD (if MODIS configured; no NDVI/NDWI for RAW)
    │       │   └── /gpm/rainfall_daily (if GPM configured; no accum for RAW)
    │       └── fusion_metadata.json (bbox, CRS, shapes, temporal_offsets, checksums)
    │
    └── processed/                      ← Fusion using PROCESSED-tier inputs (if any source has PROCESSED configured)
        └── {date}/
            ├── fusion_{date}_processed.h5
            │   ├── /sentinel1/VV, /sentinel1/VH
            │   ├── /modis/FLOOD, /modis/NDVI, /modis/NDWI
            │   └── /gpm/rainfall_24h, /gpm/rainfall_72h, /gpm/rainfall_7d
            └── fusion_metadata.json
```

**Key structural decisions**:
1. **Dimension**: `{source}/{processing_level}/{tier}/` (not `{tier}/{source}/`)
2. **Only show what's configured**: If dataset has no S1, no `sentinel1/` folder. If S1[RAW-only], no `processed/` subfolder.
3. **RAW tier naming**: RAW-level artifacts live in BRONZE (the minimum usable tier). No separate "RAW" tier folder; use folder name `raw/` under source.
4. **Fusion split**: If dataset has both S1[RAW] and S1[PROCESSED], fusion produces TWO HDF5 files: `fusion/raw/` and `fusion/processed/`, each with conditional groups based on source configs.
5. **Preview location**: Per-source-per-processing, not global. Grayscale/Colored/Composite split into subfolders per kind.

## Sentinel-1 Pipeline

### What RAW means for Sentinel-1
Calibrate (DN → sigma-nought dB) + reproject (GCP → EPSG:4326) + crop to AOI. **No speckle filtering, no quality analytics.** Output at BRONZE tier. This is the minimum to get a usable georeferenced raster — you can't skip calibration because raw DN values are meaningless without the sigma-nought LUT.

### What PROCESSED means for Sentinel-1
Full chain: calibrate + crop + **Lee filter 7×7** + **QA analytics** + **COG export**. Output through SILVER → GOLD tiers. This is where the ablation study value lives — comparing ML model performance on unfiltered (RAW) vs filtered (PROCESSED) S1 inputs.

### Stages

**Stage 1: DOWNLOAD**
- Input: CDSE OData query (bbox, date range, GRD/IW filter)
- Process: OAuth2 auth → discover scenes → download SAFE ZIP (HTTP Range-resume) → extract VV/VH GeoTIFFs → MD5 verify
- Output: `RAW/sentinel1/{product_identifier}/`
- DB: Insert `satellite_scenes` row
- Retry: 3 attempts, exponential backoff

**Stage 2: CALIBRATE**
- Input: SAFE zip + raw GeoTIFFs
- Process: Parse calibration LUT XML → apply sigma-nought (DN → dB) via `RegularGridInterpolator` → reproject to EPSG:4326 using embedded GCPs
- Output: `_work/{product_id}/` (temporary)

**Stage 3: CROP**
- Input: Calibrated rasters
- Process: Clip to dataset bbox via `rasterio.mask.mask`
- Output: `BRONZE/sentinel1/{product_id}/`
- Cleanup: Deletes `_work/` temporary directory
- **If RAW only**: Pipeline stops here for this source. BRONZE = S1 RAW artifact.

**Stage 4: LEE_FILTER** *(PROCESSED only)*
- Input: BRONZE rasters
- Process: Adaptive Lee speckle filter — `scipy.ndimage.uniform_filter` 7×7 window, 1 look
- Output: `SILVER/sentinel1/{product_id}/`
- Config: `lee_filter_window` (default 7), `lee_filter_looks` (default 1)

**Stage 5: QUALITY_ANALYTICS** *(PROCESSED only)*
- Input: SILVER (filtered) rasters
- Process per band: nodata%, backscatter stats, speckle_index (std/|mean|), radiometric check (−35 to +5 dB)
- Score: `50×(1−nodata%) + 30×max(0, 1−speckle) + 20×(radiometric_ok ? 1 : 0)` → 0–100
- Output: `SILVER/{product_id}/metadata_qa.json`
- DB: Insert `quality_metrics`, `alert_events` (if score < 60)

**Stage 6: GOLD_EXPORT** *(PROCESSED only)*
- Input: SILVER rasters
- Process: Rewrite as Cloud-Optimized GeoTIFF (512×512 tiles, overviews [2,4,8,16], LZW compression)
- Output: `GOLD/sentinel1/{product_id}/`
- DB: Insert `data_products` (tier=GOLD), `data_lineage` (SILVER→GOLD)

## MODIS Pipeline

### What RAW means for MODIS
Download HDF4 + extract **flood map only** (MCDWD_L3_F2_NRT categorical) + reproject sinusoidal → EPSG:4326 + mosaic tiles + crop AOI. **No derived indices computed.** Output at BRONZE tier.

### What PROCESSED means for MODIS
Full chain: flood map + **compute NDVI** from MOD09GA reflectance `(B02_NIR − B01_Red) / (B02 + B01)` + **compute NDWI** (McFeeters) `(B04_Green − B02_NIR) / (B04 + B02)`. Indices computed on native sinusoidal grid before reprojection. Output through SILVER → GOLD tiers (COG).

### Stages

**DOWNLOAD + EXTRACT**
- Input: LAADS DAAC query (tiles h30v08/h31v08, date)
- Auth: NASA Earthdata Bearer token
- Process: Download HDF4 → extract subdatasets
- For RAW: extract FLOOD only → reproject → crop → write BRONZE
- For PROCESSED: extract FLOOD + reflectance bands → compute NDVI/NDWI → reproject → crop → write SILVER → COG export to GOLD
- Non-fatal: Failures don't block the pipeline

**Lineage transformation types**: `EXTRACT_FLOOD`, `COMPUTE_NDVI`, `COMPUTE_NDWI`, `GOLD_EXPORT`

## GPM IMERG Pipeline

### What RAW means for GPM
Download NetCDF4 (GPM_3IMERGDF v07) + extract **daily rainfall only** (single day value) + crop to AOI. **No multi-day accumulation.** Output at BRONZE tier.

### What PROCESSED means for GPM
Full chain: daily rainfall + **compute accumulation windows**: 24h (today), 72h (past 3 days), 7-day (past week). This requires downloading adjacent days' data to build the windows. Output through SILVER → GOLD tiers (COG).

### Stages

**DOWNLOAD + ACCUMULATE**
- Input: GES DISC query (date)
- Auth: NASA Earthdata Bearer token (same as MODIS)
- For RAW: download single day → extract rainfall → crop → write BRONZE
- For PROCESSED: download target day + preceding 6 days → extract rainfall → build 24h/72h/7d sums → crop → write SILVER → COG export to GOLD
- Non-fatal: Failures don't block the pipeline

**Lineage transformation types**: `EXTRACT_RAINFALL`, `ACCUMULATE_RAIN`, `GOLD_EXPORT`

## Fusion Stage

Runs only when `count(configured sources) > 1` and `fusion_strategy` is set.

### Which input tier does fusion use?

Fusion reads from the **highest available tier** for each source:
- Source configured as PROCESSED → fusion reads from GOLD
- Source configured as RAW → fusion reads from BRONZE
- Source configured as both → **two separate fusion runs** per date (one RAW, one PROCESSED), producing two HDF5 files tagged by processing_level

### Strategy: CO_OCCURRENCE
- Only fuse dates where ALL configured sources have data on the SAME day
- Skip dates with missing sources (no NaN fill)
- Produces fewer fusion files but with perfect temporal alignment

### Strategy: FULL_COVERAGE
- Fuse every date that has data from ANY configured source
- Missing sources: search ±1–2 day window; if still missing, fill layer with NaN
- Produces one fusion file per day in date range

### Strategy: HYBRID
- Daily auxiliary data (MODIS, GPM) ingested every day → fill with ±1-2 day if missing
- Sentinel-1 anchors the fusion dates (fusion only on S1 acquisition days)
- Combines temporal density of FULL_COVERAGE with S1 co-occurrence precision

### Fusion Process (all strategies)
1. Identify eligible dates per strategy
2. For each date: locate GOLD (or BRONZE for RAW-level) products for each source
3. Reproject all onto S1 reference grid (or largest-extent source if no S1)
   - Nearest-neighbor: categorical data (MODIS FLOOD)
   - Bilinear: continuous data (all others)
4. Write HDF5 — only include groups for configured sources:
   - If sentinel1 configured: `/sentinel1/VV`, `/sentinel1/VH`
   - If modis RAW: `/modis/FLOOD` only
   - If modis PROCESSED: `/modis/FLOOD`, `/modis/NDVI`, `/modis/NDWI`
   - If gpm RAW: `/gpm/rainfall_daily` only
   - If gpm PROCESSED: `/gpm/rainfall_24h`, `/gpm/rainfall_72h`, `/gpm/rainfall_7d`
5. All datasets: gzip compressed, chunked (256×256)
6. Write `fusion_metadata.json` (bbox, shape, CRS, source scene IDs, temporal offsets, strategy, processing_level per source)
7. DB: Insert `fusion_products`, `data_products` (tier=FUSION), `data_lineage` edges

## Preview Stage (Optional)

Runs only if `preview_options` is non-empty. Renders from the highest available tier (GOLD if PROCESSED, BRONZE if RAW-only).

| Option | Output |
|---|---|
| GRAYSCALE | Percentile 2–98 stretch PNG per band |
| COLORED | Per-source colormap PNG (viridis for S1, RdYlGn for NDVI, BrBG for NDWI, YlGnBu for rain) |
| COMPOSITE | False-color RGB: R=VV, G=VH, B=VV−VH (S1 only) |

Non-fatal: preview failures don't fail the scene.

## Tier Cleanup

After all stages complete, delete tiers NOT in `required_tiers` (derived from all source configs). Mark `data_products.is_valid=False` but preserve DB rows for audit.

## Live Scheduler

- Cron: Daily 02:00 Asia/Jakarta (APScheduler)
- Process: For each enabled source in `dataset_sources`, fetch data since `last_checked_at` → run that source's pipeline with its configured processing levels
- Single-process: PostgreSQL advisory lock ensures only one worker runs scheduler

## Configuration (config.json)

```json
{
  "pipeline": {
    "max_concurrent_scenes": 2,
    "retry_max_attempts": 3,
    "retry_backoff_multiplier": 2
  },
  "sentinel1": {
    "lee_filter_window": 7,
    "lee_filter_looks": 1
  },
  "quality": {
    "min_score_threshold": 60,
    "weights": { "nodata": 50, "speckle": 30, "radiometric": 20 }
  },
  "fusion": {
    "temporal_tolerance_days": 2,
    "chunk_size": 256,
    "compression": "gzip"
  },
  "preview": {
    "percentile_low": 2,
    "percentile_high": 98
  }
}
```