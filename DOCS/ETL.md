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

Fusion reads from the tier that matches each source's configured level:
- Source configured as PROCESSED → fusion reads from GOLD
- Source configured as RAW → fusion reads from BRONZE
- Source configured as both → **two separate fusion runs** per date (one RAW, one PROCESSED), producing two HDF5 files tagged by processing_level

### How many stacks per date?

A "run level" is one complete cross-source output — one HDF5 file. The rule
(implemented once, in `ProcessingPlan.output_levels()`):

| Dataset config | Runs | Why |
|---|---|---|
| Every source at one level (all RAW, or all PROCESSED) | 1 | Nothing to compare |
| **Any** source configured RAW **and** PROCESSED | 2 (RAW + PROCESSED) | This is the ablation study: two stacks identical except for that source's level |
| Mixed but no source has both (e.g. `s1[RAW] + modis[PROCESSED]`) | 1 | Two runs would contain byte-identical data — each source only has one level to offer |

Within a run, a source that does not have the run's level falls back to the
**highest level it does have**, rather than being dropped from the stack. So in
`s1[RAW,PROCESSED] + modis[PROCESSED]`, the RAW run still contains MODIS at
PROCESSED — the user asked for MODIS, and removing it would make the two stacks
differ in more than the one variable under study.

The same rule drives the PREVIEW stage, so preview folders and fusion files
always come in matching sets.

### Cross-source date matching

MODIS and GPM products are stamped one per day at midnight **UTC**; Sentinel-1
is stamped at its actual acquisition instant. Fusion picks, per layer, the
nearest daily file whose midnight is within `ALIGNMENT_WINDOW_HOURS` (24) of the
S1 acquisition time, searching offsets 0, −1, +1 day and preferring the smallest
difference among files that actually exist.

Two details that are easy to get wrong and that the matcher handles explicitly:

- **Full 24h, not half.** The descending Sentinel-1 pass over Jabodetabek lands
  around 22:50 UTC — 22.8 h from *that day's* midnight but only 1.2 h from the
  *next* day's. A ±12 h window excludes the only day the pipeline actually
  downloads (`ensure_aux_inputs_for_date` is called with the S1 date), so every
  auxiliary layer would silently fill with NaN.
- **Compare in UTC.** `acquisition_datetime` comes back from PostgreSQL in the
  database session's timezone. Deriving a date key from that value without
  converting to UTC shifts the search by one day in any deployment east of UTC —
  Asia/Jakarta included.

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
   - If sentinel1 configured: `/sentinel1/VV`, `/sentinel1/VH` (same at both levels — "RAW" for SAR still means calibrated)
   - If modis RAW: `/modis/FLOOD` only
   - If modis PROCESSED: `/modis/FLOOD`, `/modis/NDVI`, `/modis/NDWI`
   - If gpm RAW: `/gpm/rainfall_daily` only
   - If gpm PROCESSED: `/gpm/rainfall_24h`, `/gpm/rainfall_72h`, `/gpm/rainfall_7d`
5. All datasets: gzip compressed, chunked (256×256)
6. Write `fusion_metadata_{level}.json` (bbox, shape, CRS, source scene IDs, temporal offsets, strategy, processing_level per source, per-source input tier, SHA-256 of the HDF5)
7. DB: Insert `fusion_products`, `data_products` (tier=FUSION), `data_lineage` edges

An unconfigured source contributes **no group at all** — not a group full of
NaN. A consumer must be able to tell "this sensor was never requested" from
"this sensor was requested but had no data that day"; the two need different
handling at training time, and a NaN-filled group erases the distinction. A
*configured* source whose file is missing for a date does get its group, filled
with NaN (or 255 for categorical MODIS FLOOD).

### Fusion output layout

```
fusion/{YYYYMMDD}/
    fusion_{YYYYMMDD}_raw.h5            # only when a RAW run exists
    fusion_metadata_raw.json
    fusion_{YYYYMMDD}_processed.h5      # only when a PROCESSED run exists
    fusion_metadata_processed.json
```

The processing level is always part of the filename, even when a dataset only
produces one stack. Conditional naming would force every consumer — API,
training notebook, third-party script — to handle two patterns and guess which
applies from a dataset config it may not have.

The HDF5 root carries `processing_level`, `sources`, `source_levels`,
`fusion_strategy`, `crs`, `crs_wkt`, `transform`, and `layers` as attributes, so
a stack describes itself without its sidecar.

### Fusion rows in `data_products`

Each stack is registered with `band_name = FUSION_RAW` or `FUSION_PROCESSED`.
The level belongs in the band name because the `is_latest` de-duplication key is
`(scene_id, band_name, product_tier, dataset_id)`: with a shared `FUSION` band
name, registering the PROCESSED stack would mark the RAW stack of the same date
superseded, and it would vanish from every API listing.

### Fusion requires Sentinel-1

Fusion in this pipeline is S1-anchored: the S1 scene supplies both the reference
grid every other layer is reprojected onto and the date being fused. A dataset
that configures only MODIS + GPM with a fusion strategy raises rather than
writing a grid-less stack. See `DOCS/IMPLEMENTATION_NOTES.md`.

## Preview Stage (Optional)

Runs only if `generate_preview` is true **and** `preview_options` is non-empty.

Renders once per run level (the same `ProcessingPlan.output_levels()` set fusion
uses), reading the tier that matches that level:

| Run level | Reads from | S1 file pattern |
|---|---|---|
| PROCESSED | `gold/` | `*_{BAND}_lee.tif` |
| RAW | `bronze/` | `*_{BAND}_crop.tif` |

A RAW-level render produces fewer layers — not because anything failed, but
because MODIS NDVI/NDWI and GPM 72h/7d are never computed on the RAW path.
Missing layers are reported in `skipped` with a reason.

| Option | Output dir | Content |
|---|---|---|
| GRAYSCALE | `grayscale/` | Percentile 2–98 stretch PNG per band |
| COLORED | `colored/` | Per-source colormap PNG (viridis for S1, RdYlGn for NDVI, BrBG for NDWI, YlGnBu for rain) |
| COMPOSITE | `composite/` | False-color RGB: R=VV, G=VH, B=VV−VH (S1 only) |

### Preview output layout

```
preview/{YYYYMMDD}/
    preview_metadata.json               # copy of the default level, for older readers
    {RAW|PROCESSED}/
        preview_metadata.json
        grayscale/   {key}.png + grayscale_info.json
        colored/     {key}.png + colored_info.json
        composite/   s1_rgb_composite.png + composite_info.json
```

The level sits in the path because both levels write the same filenames
(`s1_vv.png`); without a separating folder the second render overwrites the
first. The composite gets its own folder rather than sitting in `colored/`
because it combines three bands instead of colouring one — no colormap or single
value range describes it, so it cannot share `colored_info.json`'s schema.

API: `GET /api/datasets/{id}/preview` returns `by_level`, `processing_levels`,
and `default_processing_level` (PROCESSED when present), with `kinds` at the top
level still pointing at the default level for older clients. Images are served
from `/preview/{scene}/{level}/{kind}/{filename}`; the older level-less URL still
resolves, to the default level.

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
