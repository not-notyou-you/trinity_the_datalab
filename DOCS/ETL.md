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

### On-disk layout

```
data/datasets/{id}_{slug}/
├── metadata.json
├── sentinel-1/{RAW,PROCESSED}/      # e.g. S1A_..._20240305T111407_VV_crop.tif
├── modis/{RAW,PROCESSED}/           # e.g. modis_20240305_ndvi.tif
├── gpm-imerg/{RAW,PROCESSED}/       # e.g. gpm_rain_24h_20240305.tif
├── fusion/{co-occurrence,full-coverage,hybrid}/
├── preview/{RAW,PROCESSED}/{grayscale,colored,composite}/
├── _granule_cache/{modis,gpm}/      # raw NASA granules, shared across dates
└── _work/                           # scratch, swept at end of every job
```

**Key structural decisions**:

1. **Source first, two drawers each.** The path uses exactly the vocabulary the
   user picked at creation time (`sentinel1: [RAW, PROCESSED]`), so "give me
   Sentinel-1 PROCESSED only" is one folder rather than files scattered across
   one folder per date.
2. **Dates live in filenames, not folders.** Every writer already embedded the
   date (S1 carries the full `product_identifier`), so `list_dates` reads it
   back from the filename. Preview PNGs get an explicit date prefix —
   without it the second date would overwrite the first.
3. **Tier is no longer a path segment**, but it is *not* gone: it remains the
   value of `data_products.product_tier`, the vocabulary of `data_lineage`,
   and the key of `storage_breakdown` (DOCS/DECISIONS.md D14). The mapping is
   `BRONZE -> {source}/RAW/` and `GOLD -> {source}/PROCESSED/`.
4. **Intermediate artifacts are not retained.** The SAFE zip (tier RAW) and the
   Lee-filtered raster before COG export (tier SILVER) have no drawer: they
   live in `_work/` and are swept when the job ends. The cost is explicit —
   re-running the Lee filter with different parameters means re-downloading the
   scene. In exchange, `{source}/RAW/` vs `{source}/PROCESSED/` means exactly
   what D2 says it means, with nothing else competing for the same folder.
   QA metrics survive in the `quality_metrics` table; only the
   `metadata_qa.json` sidecar is transient.
5. **Only what is configured exists.** No `modis/` folder if MODIS was never
   requested; no `PROCESSED/` drawer for a RAW-only source.
6. **Fusion and preview are cross-source and cross-date.** Both sit at the
   dataset root: one folder holding the whole time series is the shape a
   consumer actually wants to stack. Fusion splits by strategy; preview splits
   by processing level then by render kind.
7. **Datasets created before the relayout are not migrated.** They keep the old
   `{YYYYMMDD}/{tier}/{source}/` tree, `folder_manager.is_legacy_layout()`
   detects them, and the UI shows a "format lama" notice instead of rendering a
   tree with vocabulary that no longer applies. Their files stay downloadable.

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
Download HDF4 + extract **flood map only** (MCDWD_L3_F2_NRT categorical, subdataset `Flood_2Day_250m` — 2-day composite; the 1-day layer is not used because cloud shadows and dark urban pixels leak through as "Flood (unusual)") + reproject → EPSG:4326 + mosaic tiles + crop AOI. **No derived indices computed.** Output at BRONZE tier.

### What PROCESSED means for MODIS
Full chain: flood map + **compute NDVI** `(B02_NIR − B01_Red) / (B02 + B01)` + **compute NDWI** (McFeeters) `(B04_Green − B02_NIR) / (B04 + B02)` from **MOD09A1** 8-day surface-reflectance composites (standard archive only; the composite whose 8-day period contains the target date is used, and its period is recorded as `composite_period`). If that composite is not published yet, it falls back to daily **MOD09GA_NRT**. Indices computed on native sinusoidal grid before reprojection. Pixels flagged in the State QA (`sur_refl_state_500m` / `state_1km`) as cloudy/mixed, cloud shadow, cirrus (average/high), internal cloud, or fill are set to NaN before reprojection — persistently cloudy dates can still end up NaN, which is correct (fusion fills NaN). Output through SILVER → GOLD tiers (COG).

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

Output grid (both levels): exactly the dataset AOI bbox at ~10 m, **nearest-neighbour** from the native 0.1° IMERG cells (so each ~11 km cell stays a visible block instead of an invented bilinear gradient), float32 + DEFLATE.

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

### Strategies: two axes, not one

A strategy answers **two separate questions**, and they are what actually
distinguish the three. Implemented in `etl/fusion_strategies.py`
(`plan_fusion()` returns a `FusionPlan` carrying both axes).

| Strategy | Axis 1 — DOWNLOAD (which aux dates are fetched) | Axis 2 — ASSEMBLE (which dates become an HDF5) |
|---|---|---|
| `CO_OCCURRENCE` | S1 acquisition dates only | one file per S1 date, same-day aux |
| `FULL_COVERAGE` | every day in range | one file per day; S1 borrowed from nearest date within tolerance, NaN if none |
| `HYBRID` | every day in range | one file per S1 date; aux uses the daily density around the anchor |

`HYBRID` is a genuine third strategy precisely because it picks a different
axis from each of the others — it **downloads like FULL_COVERAGE and assembles
like CO_OCCURRENCE**. It does *not* mean "generate both of the other two".

**S1 match tolerance** (`FULL_COVERAGE` only): default ±2 days
(`DEFAULT_S1_MATCH_TOLERANCE_DAYS`). Sentinel-1A alone revisits every ~12 days
at the equator, so requiring same-day S1 would leave most days without it. On
a tie the earlier scene wins, deterministically — an arbitrary pick would make
the same dataset produce different output between runs. The actual gap is
always recorded so consumers can filter more strictly afterwards.

**Days with no S1 at all** (`FULL_COVERAGE` only): the file is still written.
The `sentinel1/` group is present but filled with NaN — the same treatment
missing MODIS/GPM already get. The group is *not* omitted, because
`fusion_layers_for` uses group absence to mean "this sensor was never
requested", which is a different situation from "requested but missing that
day". The reference grid falls back to the AOI bbox at the same resolution
`module8_gpm_download` already uses for its "Sentinel-1 grid", so S1-less days
land on exactly the same grid as S1 days and the series can still be stacked.

### Output layout

Each strategy writes to its own subfolder, and the strategy also appears in
the filename:

```
{date}/fusion/co-occurrence/fusion_{date}_cooccurrence_{level}.h5
{date}/fusion/full-coverage/fusion_{date}_fullcoverage_{level}.h5
{date}/fusion/hybrid/fusion_{date}_hybrid_{level}.h5
```

Without this separation, re-running a dataset under a different strategy would
overwrite the previous result — and comparing strategies is the whole point
(D1). The suffix is repeated in the filename because a file downloaded and
moved elsewhere loses its folder context.

### Fusion Process (all strategies)
1. Identify eligible dates per strategy (`plan_fusion()` — both axes above)
2. For each date: locate GOLD (or BRONZE for RAW-level) products for each source
3. Reproject all onto S1 reference grid (or largest-extent source if no S1)
   - Nearest-neighbor: categorical data (MODIS FLOOD) and GPM rainfall (0.1° cells kept as blocks)
   - Bilinear: MODIS NDVI/NDWI
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

Ordering: runs after the S1 chain and `ensure_aux_inputs_for_date`, before FUSION and before tier cleanup (the only window where every source raster for the date is still on disk). Pause/cancel is re-checked right before rendering. PNGs are recorded per level, so a failure at the second level keeps the first level's files in the job accounting.

One set per date: preview folders are keyed by date, not by scene. A second Sentinel-1 scene on the same date overwrites the first scene's PNGs at the same level. This is intentional; module10 logs a WARNING and records the replaced scene in `replaced_s1_scene_key` in `preview_metadata.json`.

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