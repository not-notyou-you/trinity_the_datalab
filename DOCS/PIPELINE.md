# Pipeline

What the ETL pipeline does, stage by stage, and why it's built the way it is. (Merged from the former `ETL.md` + `IMPLEMENTATION_NOTES.md` — see DOCS/README.md for the current doc set.) The first half describes what the pipeline does; "Implementation Notes" at the end describes why it does it that way and what will bite you if you change it.

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

## On-disk layout

```
data/datasets/{id}_{slug}/
├── metadata.json
├── sentinel-1/{RAW,PROCESSED}/      # e.g. S1A_..._20240305T111407_VV_crop.tif
├── modis/{RAW,PROCESSED}/           # e.g. modis_20240305_ndvi.tif
├── gpm-imerg/{RAW,PROCESSED}/       # e.g. gpm_rain_24h_20240305.tif
├── fusion/{co-occurrence,full-coverage,hybrid}/
├── preview/{RAW,PROCESSED}/{grayscale,colored,composite}/
├── masks/                            # reference layers: land_distance.tif, water_occurrence.tif (dataset-wide, not per-date)
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

## Tier vocabulary used below

Stage descriptions below use the **current, D14 tier names** — `RAW`, `ALIGNED`, the per-source rank-2 name (`DESPECKLED` for S1, `INDICES` for MODIS, `ACCUMULATED` for GPM), and `COG` — matching what the code actually writes to `data_products.product_tier` and to `folder_manager` calls (e.g. `fm.get_scene_dir(..., "aligned", ...)`, `fm.ensure_scene_dir(..., "cog", ...)`). The pre-D14 medallion names (`BRONZE`/`SILVER`/`GOLD`) are legacy — never written anymore, only read from rows created before the rename. `etl/tier_names.py` is the single source of truth for this vocabulary and the mapping between the two (`rank()`, `equivalent_tiers()`, `canonical_tier()`); see DOCS/DECISIONS.md D14/D15 for the rationale.

## Sentinel-1 Pipeline

### What RAW means for Sentinel-1
Calibrate (DN → linear sigma0) + reproject (GCP → EPSG:4326) + crop to AOI. **No speckle filtering, no quality analytics.** Output at ALIGNED tier. This is the minimum to get a usable georeferenced raster — you can't skip calibration because raw DN values are meaningless without the sigma-nought LUT.

### What PROCESSED means for Sentinel-1
Full chain: calibrate + crop + **Lee filter 7×7** + **QA analytics** + **COG export**. Output through DESPECKLED → COG tiers. This is where the ablation study value lives — comparing ML model performance on unfiltered (RAW) vs filtered (PROCESSED) S1 inputs.

### Stages

**Stage 1: DOWNLOAD**
- Input: CDSE OData query (bbox, date range, GRD/IW filter)
- Process: OAuth2 auth → discover scenes → download SAFE ZIP (HTTP Range-resume) → extract VV/VH GeoTIFFs → MD5 verify
- AOI coverage gate: before any download, the orchestrator unions the catalogue footprints of all frames on a date and drops the date when they cover less than `MIN_S1_AOI_COVERAGE` (5%) of the AOI. Decided per date, not per frame, so both halves of a split pass are kept. Scenes without a readable footprint are never dropped. (24_try8 downloaded 1.6 GB for a 14 Jan pass that covered 0.5% of the AOI and fused a 455 MB stack that was 99.4% NaN.)
- Output: `RAW/sentinel1/{product_identifier}/`
- DB: Insert `satellite_scenes` row
- Retry: 3 attempts, exponential backoff
- Reuse: `etl/download_guard.py` lets a scene already downloaded for one dataset be hardlinked/copied into a new dataset's folder instead of re-fetched (`find_reusable_file`), and detects a stalled/dead connection via a minimum-throughput window (`DOWNLOAD_MIN_KBPS`/`DOWNLOAD_STALL_WINDOW_S`) rather than only a hard timeout.

**Stage 2: CALIBRATE**
- Input: SAFE zip + raw GeoTIFFs
- Process: Parse calibration LUT XML → apply sigma-nought via `RegularGridInterpolator` → reproject to EPSG:4326 using embedded GCPs
- Output: `_work/{product_id}/` (temporary)
- **The result is linear sigma0, not dB** (`etl/module1b_calibrate.py`: `sigma0 = DN² / sigma_lut²`). Every stage downstream — Lee filter, quality analytics' backscatter stats, the fusion HDF5's `/sentinel1/VV`/`VH` layers — operates on this linear value. Conversion to dB (`10·log10(sigma0)`) happens only inside `module6_analytics.py`'s QA computation, as an internal step, never as a separate stored artifact.

**Stage 3: CROP**
- Input: Calibrated rasters
- Process: Clip to dataset bbox via `rasterio.mask.mask`
- Output: `ALIGNED/sentinel1/{product_id}/`
- Cleanup: Deletes `_work/` temporary directory
- **If RAW only**: Pipeline stops here for this source. ALIGNED = S1 RAW artifact.

**Stage 4: LEE_FILTER** *(PROCESSED only)*
- Input: ALIGNED rasters
- Process: Adaptive Lee speckle filter — `scipy.ndimage.uniform_filter` 7×7 window, 1 look
- Output: `DESPECKLED/sentinel1/{product_id}/`
- Config: window/looks are hardcoded in `etl/module3_lee_filter.py` (7×7, 1 look) — not read from `config.json` (see "Configuration" below)

**Stage 5: QUALITY_ANALYTICS** *(PROCESSED only)*
- Implemented in `etl/module6_analytics.py`
- Input: DESPECKLED (filtered) rasters
- Process per band: nodata%, backscatter stats (dB, converted internally from the linear values), speckle_index (std/|mean|), radiometric check (−35 to +5 dB)
- Score: `50×(1−nodata%) + 30×max(0, 1−speckle) + 20×(radiometric_ok ? 1 : 0)` → 0–100 — these weights are **hardcoded constants** in `compute_quality_score()`, not read from any config file
- Output: `DESPECKLED/{product_id}/metadata_qa.json`
- DB: Insert `quality_metrics`, `alert_events` (if score < 60)

**Stage 6: COG_EXPORT** *(PROCESSED only, `etl/module4_gold_export.py`)*
- Input: DESPECKLED rasters
- Process: Rewrite as Cloud-Optimized GeoTIFF via GDAL's COG driver, 512-px blocksize, `DEFLATE` compression with `predictor=YES` — **not LZW**. Overview resampling is chosen per file: `average` for continuous data (backscatter, NDVI/NDWI, rainfall), `nearest` for categorical uint8/int8 data (MODIS FLOOD) — so a flood-class raster's overviews don't invent fractional "in-between" classes.
- `config.json`'s `pipeline.cog_compression: "LZW"` key is **dead** — `module4_gold_export.py` never reads it and always uses DEFLATE regardless of what the config file says.
- Output: `COG/sentinel1/{product_id}/`
- DB: Insert `data_products` (tier=COG), `data_lineage` (DESPECKLED→COG)

## MODIS Pipeline

### What RAW means for MODIS
Download HDF4 + extract **flood map only** (MCDWD_L3_F2_NRT categorical, primarily the 2-day composite subdataset `Flood_2Day_250m`) + reproject → EPSG:4326 + mosaic tiles + crop AOI. **No derived indices computed.** Output at ALIGNED tier.

The 2-day layer is not used blindly, though: `etl/module7_modis_download.py` fills gaps in it from the **cloud-shadow-masked 1-day layer** (`FloodCS_1Day_250m` — never the plain, unmasked `Flood_1Day_250m`, which is the one that leaks cloud shadow/dark-urban false positives). Each output pixel's origin is recorded in a second band (`FLOOD_SOURCE_2DAY` / `FLOOD_SOURCE_1DAY_CS`), so a consumer can tell which rule produced it.

### What PROCESSED means for MODIS
Full chain: flood map + **compute NDVI** `(B02_NIR − B01_Red) / (B02 + B01)` + **compute NDWI** (McFeeters) `(B04_Green − B02_NIR) / (B04 + B02)` from **MOD09A1** 8-day surface-reflectance composites. Rather than picking one 8-day period containing the target date, `_composite_latest_clear()` builds a **per-pixel latest-clear-observation composite** across up to `INDEX_LOOKBACK_DAYS` (32) days of MOD09A1 periods — for each pixel, the most recent cloud-free observation at or before the target date wins, never a future one. If the most recent period hasn't been published to the archive yet, that period falls back to daily **MOD09GA_NRT**. Indices computed on native sinusoidal grid before reprojection. Pixels flagged in the State QA (`sur_refl_state_500m` / `state_1km`) as cloudy/mixed, cloud shadow, cirrus (average/high), internal cloud, or fill are set to NaN before reprojection — persistently cloudy dates can still end up NaN, which is correct (fusion fills NaN). Output through INDICES → COG tiers.

### Stages

**DOWNLOAD + EXTRACT**
- Input: LAADS DAAC query (tiles h30v08/h31v08, date)
- Auth: NASA Earthdata Bearer token
- Process: Download HDF4 → extract subdatasets
- For RAW: extract FLOOD (2-day, gap-filled from cloud-shadow-masked 1-day) → reproject → crop → write ALIGNED
- For PROCESSED: extract FLOOD + reflectance bands → compute NDVI/NDWI (per-pixel latest-clear composite) → reproject → crop → write INDICES → COG export to COG
- Non-fatal: Failures don't block the pipeline

**Lineage transformation types**: `EXTRACT_FLOOD`, `COMPUTE_NDVI`, `COMPUTE_NDWI`, `GOLD_EXPORT`

## GPM IMERG Pipeline

### What RAW means for GPM
Download NetCDF4 + extract **daily rainfall only** (single day value) + crop to AOI. **No multi-day accumulation.** Output at ALIGNED tier.

The specific IMERG product used **falls through three runs** in order, per `etl/module8_gpm_download.py`'s `IMERG_RUN_ORDER = ["F", "L", "E"]`: **Final Run** (`GPM_3IMERGDF`, the fully gauge-corrected product) first, then **Late Run** (`GPM_3IMERGDL`), then **Early Run** (`GPM_3IMERGDE`) if Final hasn't been published for that date yet. On GES DISC, Final Run V07 currently lags the present by several months, so live/recent dates routinely resolve to Late or Early Run instead. The metadata records which run was actually used, tagged `GOOD` (Final), `LATE_RUN`, or `DEGRADED` (Early) — a consumer comparing rainfall values across dates should check this rather than assuming uniform product quality.

### What PROCESSED means for GPM
Full chain: daily rainfall + **compute accumulation windows**: 24h (today), 72h (past 3 days), 7-day (past week). This requires downloading adjacent days' data to build the windows. Output through ACCUMULATED → COG tiers.

Output grid (both levels): exactly the dataset AOI bbox at ~10 m, **nearest-neighbour** from the native 0.1° IMERG cells (so each ~11 km cell stays a visible block instead of an invented bilinear gradient), float32 + DEFLATE.

### Stages

**DOWNLOAD + ACCUMULATE**
- Input: GES DISC query (date), Final → Late → Early Run fallback as above
- Auth: NASA Earthdata Bearer token (same as MODIS)
- For RAW: download single day → extract rainfall → crop → write ALIGNED
- For PROCESSED: download target day + preceding 6 days → extract rainfall → build 24h/72h/7d sums → crop → write ACCUMULATED → COG export to COG
- Non-fatal: Failures don't block the pipeline

**Lineage transformation types**: `EXTRACT_RAINFALL`, `ACCUMULATE_RAIN`, `GOLD_EXPORT`

## Fusion Stage

Runs only when `count(configured sources) > 1` and `fusion_strategy` is set.

### Which input tier does fusion use?

Fusion reads from the **highest available tier** for each source, via `etl/processing_plan.py`'s `SourcePlan.tier_for_run()`:
- Source configured as PROCESSED → fusion reads from COG
- Source configured as RAW → fusion reads from ALIGNED
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
2. For each date: locate COG (or ALIGNED for RAW-level) products for each source
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
6. Write the sidecar `fusion_{date}_{strategy}_{level}_metadata.json` (bbox, shape, CRS, source scene IDs, temporal offsets, strategy, processing_level per source). The date is in the name because the fusion folder is shared by all dates: the old `fusion_metadata_{level}.json` was overwritten by every date (24_try8 kept one sidecar for three HDF5 files)
   - The same provenance is also written as HDF5 attributes. Root: `feature_date`, `acquisition_datetime_utc`, `s1_offset_days`, `temporal_offset_modis/gpm`. Per layer: `units` (S1 is **linear sigma0, not dB**), `source_date`, `day_offset`, `valid_fraction`, the source file's provenance tags (`source_product`, `composite_start/end` for the 8-day MOD09A1 NDVI/NDWI, `window_start_utc/end_utc` for GPM), and for GPM `hours_after_s1_acquisition`
   - MODIS/GPM are taken from the feature date D, or D-1 as a fallback, **never D+1** (see "Implementation Notes" §3.4 below)
7. DB: Insert `fusion_products`, `data_products` (tier=FUSION), `data_lineage` edges

## Preview Stage (Optional)

Runs only if `preview_options` is non-empty. Renders from the highest available tier (COG if PROCESSED, ALIGNED if RAW-only).

| Option | Output |
|---|---|
| GRAYSCALE | Percentile 2–98 stretch PNG per band |
| COLORED | Per-source colormap PNG (viridis for S1, RdYlGn for NDVI, BrBG for NDWI, YlGnBu for rain) |
| COMPOSITE | False-color RGB: R=VV, G=VH, B=VV−VH (S1 only) |

Non-fatal: preview failures don't fail the scene.

Ordering: runs after the S1 chain and `ensure_aux_inputs_for_date`, before FUSION and before tier cleanup (the only window where every source raster for the date is still on disk). Pause/cancel is re-checked right before rendering. PNGs are recorded per level, so a failure at the second level keeps the first level's files in the job accounting.

Sidecars are per date: `{date}_preview_metadata.json` and `{date}_{kind}_info.json`. One preview folder holds every date of the dataset (files carry a date prefix), so an unprefixed sidecar was rewritten in full by each scene and the gallery ended up showing the last-rendered date's images for every date (dataset 22_try6). The gallery API still reads the old unprefixed sidecar for datasets rendered before this change, but only for the date it actually describes.

PNG size is capped on the LONGEST side, not the width. A Sentinel-1 scene that only clips the AOI produces a narrow strip (1488 x 8789 px in 22_try6); capping width alone rendered it 1024 x 6048 — six times taller than the intended limit.

One scene set per date: an AOI longer than a single Sentinel-1 frame is covered by two scenes from the same pass. They are mosaicked (`etl/s1_mosaic.py`) before PREVIEW and FUSION run, so the date produces one output covering the whole AOI instead of the later frame silently overwriting the earlier one.

## Per-date finalization

PREVIEW and FUSION do not run inside the per-scene pipeline. Both write files named by DATE only (`20250123_s1_vv.png`, `fusion_20250123_hybrid_processed.h5`), while one date can hold several Sentinel-1 scenes. Run per scene, the scene that finished last overwrote the one that finished first — in dataset 22_try6 a frame covering 68.7% of the AOI was overwritten by one covering 51.7%, and half the AOI disappeared from the deliverable.

The pipeline worker therefore collects finished scenes per date and finalizes a date once every one of its scenes has been processed (`_finalize_date`): mosaic the frames, render PREVIEW once, write FUSION once. Cleanup is queued only after that, because for `fusion_output_only` datasets cleanup deletes the very rasters those stages read. A cancelled job leaves unfinalized dates alone rather than writing half a date's data.

## Fusion Grid

Every stack in a dataset is assembled on ONE grid: the AOI box at the resolution of the dataset's own Sentinel-1 rasters (`_dataset_fusion_grid`). Previously each S1 day used its own scene footprint, so a single month produced five different shapes (22_try6: 8789x1488, 6067x8752, 8790x1483, 6068x8752, 8790x1492), none of them pixel-aligned with the others — the time series could not be stacked into one array, which is the point of the FUSION tier. Some covered only 17% of the AOI while their `aoi_bbox` attribute promised the full AOI.

Days whose scene only clips the AOI now write a full-AOI raster that is mostly NaN. That is cheap on disk (gzip collapses NaN blocks) and bounded in memory: layers are written one at a time as they are computed (`_FusionH5Layers`) instead of being accumulated in a dict.

Root attributes added: `grid_bbox` (the raster's real bounds, next to the requested `aoi_bbox`) and `grid_offset_row_col` (its position inside the AOI box, in pixels).

Every layer's valid-pixel fraction is recorded in `layer_coverage` in the sidecar JSON, and layers below 5% valid pixels are logged as a WARNING. Three of the five dates in 22_try6 held 3.6-3.7% valid Sentinel-1 pixels and were reported as plain successes.

## Tier Cleanup

After all stages complete, delete tiers NOT in `required_tiers` (derived from all source configs). Mark `data_products.is_valid=False` but preserve DB rows for audit.

This is a much smaller mechanism than **deleting a dataset**, which is `etl/deletion_manager.py`, not this stage. `DeletionManager` handles `delete_all()` (every file + every DB row, `is_deletable` guard), `clear_files_only()`, and `delete_tiers()`, all resumable via a `.deletion_manifest.json` written as files are removed — a crash mid-delete can restart from where it left off instead of re-scanning everything. It also removes the orphaned `NASA_AUX_*` placeholder `satellite_scenes` rows (see DOCS/ARCHITECTURE.md's `data_products.scene_id` note) that a dataset's MODIS/GPM products were registered against, and tracks progress in the `cleanup_operations` table (`GET /api/datasets/{id}/deletion-progress`).

## Location Resolution

`etl/location_resolver.py` turns a `CreateDatasetRequest`'s `region_id` or free-text `location` into a concrete bbox: `region_id` looks up a `regions_of_interest` row directly; free text is resolved by name first, then geocoded via Nominatim (`etl/geo_utils.py`'s `geocode_search()`) if no name matches. A geocoded location that isn't already a saved region gets auto-created as one (`source='USER'`), including reviving a previously soft-deleted region with the same name rather than creating a duplicate. `geo_utils.py` also holds the shared bbox sanity bounds (`MAX_SPAN_DEG=10.0`, `MIN_SPAN_DEG=0.001`) used by both the API layer and this resolver.

## Live Scheduler

`etl/live_scheduler.py`, `APScheduler.BackgroundScheduler`, started unconditionally in `api/main.py` on process startup.

- Cron: Daily 02:00 Asia/Jakarta.
- **Not the same codepath for every source.** SENTINEL1 goes through the full per-scene orchestrator job (`run_dataset_job`) — the same pipeline a normal dataset job runs. MODIS and GPM instead go through a lighter day-by-day loop that calls `ensure_modis_inputs_for_date()`/`ensure_gpm_inputs_for_date()` directly (helpers also used by fusion), not the full orchestrator.
- **No advisory lock.** An earlier draft of this document claimed a PostgreSQL advisory lock serializes the scheduler across API workers — there is no such lock anywhere in the codebase (`etl/live_scheduler.py` starts a plain in-process `BackgroundScheduler` with no cross-process guard). If you run more than one API worker process, nothing here stops each one from running its own copy of the live cron. `etl/job_lock.py`'s OS-level per-job file lock (see "Concurrency & Durability Safeguards" above) prevents two processes from *writing the same job's files* concurrently, but that is a different, narrower guarantee than "only one worker runs the scheduler" — it does not stop the cron from firing twice.

## Configuration

**`config/config.json` is largely a historical leftover, not the live configuration surface it looks like.** Its `area`/`area_presets` sections are marked `_DEPRECATED` in the file itself (migration 012 moved AOI selection to `regions_of_interest` via the UI/API) and are read by nothing. Its `pipeline`/`quality`/`storage_presets` sections *look* like they configure the Lee filter, COG compression, and QA thresholds, but in practice most of those keys are dead too — `module3_lee_filter.py` hardcodes the 7×7/1-look Lee filter, `module4_gold_export.py` hardcodes DEFLATE compression regardless of `pipeline.cog_compression: "LZW"`, and `module6_analytics.py`'s score weights (50/30/20) are hardcoded constants, not read from `quality.weights` (which doesn't exist in the real file to begin with). Treat `config.json` as unreliable for describing current pipeline behavior; the stage-by-stage sections above describe what the code actually does, independent of this file.

The configuration that actually takes effect at runtime is environment variables, read via `etl/config.py` and directly at call sites — see DOCS/ARCHITECTURE.md "Environment Variables (.env)" for the full list. The one that matters most for pipeline behavior:

```bash
PIPELINE_MAX_CONCURRENT_SCENES=2   # etl/module5_orchestrator.py — a threading.Semaphore, not a config.json key
```

## GPM Storage

Rainfall rasters are stored at IMERG's native 0.1 degree resolution, cropped to the AOI (every cell the AOI overlaps is kept, but not cells that only share an edge with it). The IMERG grid is snapped to exactly 0.1° with its origin at -180/90: its coordinates are stored as float32, and the resulting 0.0999999983° resolution used to pull in a ninth, all-nodata column (107.2-107.3) in dataset 24_try8. They used to be resampled to the ~10 m Sentinel-1 grid so they could be stacked directly: an AOI of 0.8 degrees holds 8x8 IMERG cells but was written as 8906x8906 pixels — 64 distinct values in 2.4 MB, 214 MB for one month in dataset 22_try6.

Nothing downstream loses alignment: both consumers reproject to their own target grid anyway (module9 to the dataset fusion grid, module10 to the preview grid), both with nearest resampling, so the result is identical.

## Reference Layers

`etl/reference_layers.py` writes two dataset-wide (not per-date) rasters into `masks/` once per dataset, on the S1 fusion grid (`datasets.fusion_grid`):

| Layer | File | Source | Meaning |
|---|---|---|---|
| Land/sea distance | `masks/land_distance.tif` | `reference_land_polygons` (OSM land polygons, ODbL, clipped to Indonesia — migration 023) | Signed distance in meters: positive on land, negative at sea, zero at the coastline. Rivers/lakes are **not** cut out — their flooding is the signal being looked for. |
| Permanent water occurrence | `masks/water_occurrence.tif` | JRC Global Surface Water (1984–2021) | Percent of the time a pixel was observed as water. Not a mask — a baseline for "how wide is this water normally," so a flood can be told apart from an ordinary channel. |

Both are optional and non-fatal — a reference-layer failure never fails a job — and are exposed by `GET /api/datasets/{id}/masks`.

**Idempotency is grid-aware, not just existence-aware.** `_ensure_land`/`_ensure_occurrence` used to return `"exists"` the moment the `.tif` was present. When several sub-datasets are later pinned to one shared `fusion_grid` (see D19 in DOCS/DECISIONS.md), a mask built before the pin silently stops matching the stack it sits next to — same file, same location, no error, wrong pixels. The guard now compares the existing file's grid against the pinned `fusion_grid` (`_grid_matches`, half-pixel tolerance for JSON round-trip error) and rebuilds automatically on mismatch (D20). `audit_dataset_grids` (`module9_fusion.py`) additionally checks every *stack's* grid against the pin on every fusion run and warns without failing — this catches stacks that predate a pin and can only be fixed by re-fusing (see Rebuilding a Date, below).

## Rebuilding a Date Without Re-downloading

`etl/refusion.py` rebuilds one date's PREVIEW + FUSION output from Sentinel-1 rasters that are **already on disk**, without touching the network.

Why this exists: a fusion stack can go stale without its source scene going stale — e.g. when several sub-datasets are pinned to one shared `fusion_grid` after some of their stacks were already assembled (D19/D20 in DOCS/DECISIONS.md), leaving those stacks on the old grid. There is otherwise no path to fix this cheaply:

- `run_dataset_job` skips any scene that is already `scene_is_done` (CLEANUP/COMPLETED), so simply re-running the job is a no-op.
- Resetting the scene's status does force a re-run, but from DOWNLOAD — and since D15 the SAFE ZIP is not retained, so that costs a full re-download (~1.7 GB/scene) for work that needs zero network bytes, because the PROCESSED-tier raster fusion actually reads is already in `sentinel-1/PROCESSED/`.

`refuse_date(db, job_id, date_key)` reconstructs the same `_JobContext` and `_SceneResult` objects `run_dataset_job` would have built, then calls the orchestrator's own `_finalize_date` — the same function that runs mosaic → preview → fusion → reference layers in the normal pipeline — rather than calling `create_fusion_stack` directly. This matters because a date covered by more than one S1 frame (mosaic case) would otherwise get a stack containing only one frame if fusion were invoked directly with a single `scene_id`.

**Deliberate limit**: only works for dates whose S1 scene finished processing and whose PROCESSED-tier rasters are still on disk. A date whose rasters have already been swept must go through the full pipeline; `refuse_date` refuses rather than producing a half stack.

Not wired to an API endpoint or UI button as of this writing — invoked directly (e.g. from a maintenance script or REPL) against a `job_id` and `date_key`.

## Cross-Dataset Merge

`etl/dataset_merge.py`, `api/routes/merge.py` (`/api/merge/*`), "Gabungkan Dataset" panel in the web UI.

Splitting a large AOI into several bbox strips (D16 in DOCS/DECISIONS.md — a single rectangle covering an island like Jawa is majority sea) leaves N separate FUSION stacks per date, one per sub-dataset. This module stitches them back into one array per date for consumers who need to work across the original AOI.

**Stitches, does not resample.** Sub-dataset strips are cut with boundaries locked to a shared `fusion_grid` (D16/D19), so adjacent strips' pixel grids are guaranteed to align on integer pixel offsets — merging is just placing blocks at the right offset. Resampling is deliberately avoided: these stacks store Sentinel-1 backscatter as **linear sigma0**, not dB, and interpolating in linear space is especially misleading exactly at the land/water boundary where flood signal is read. A misaligned pair of grids is *rejected* (`GridMismatch`), not silently resampled to fit.

**Deliberate limits:**
- Only FUSION-tier HDF5 stacks are merged; per-satellite intermediates are not (much larger, not a deliverable).
- The layer set must match exactly across inputs — a layer missing from one strip fails the merge rather than being filled with NaN, since a layer silently missing over half the map is a trap for consumers, not a graceful degradation.
- Only same-date stacks are merged together.

**Two-step, LOOK then ALLOW**: `GET /api/merge/candidates` never writes anything — it reports which dates have stacks in more than one dataset, whether their grids actually line up, and why not when they don't. `POST /api/merge/run` never guesses which datasets to merge; the caller must name `dataset_ids` explicitly, because the candidate list can change between when the UI showed it and when the user clicks the button. Source datasets are never modified or deleted by a merge. Output lands in `data/merged/merged_{date}.h5`, a sibling of `data/datasets/`, not inside it — anything under `data/datasets/` is read by the storage scanner as a dataset folder.

## Concurrency & Durability Safeguards

See also DOCS/DECISIONS.md D22. Two small modules close races that only show up under `uvicorn --reload` or when a process is killed mid-write:

**`etl/job_lock.py`** — an OS-level file lock (`msvcrt.locking` on Windows, `fcntl.flock` on POSIX) exclusive per `job_id`, held in `<data_root>/../_job_locks/job-{id}.lock`. `dataset_manager`'s in-process thread registry only prevents two threads in the *same* process from working the same job; it does nothing when `uvicorn --reload` starts a new process while the old one is still shutting down, and the new process's `recover_interrupted_jobs()` resumes a job the old process is still writing. Two processes writing the same files corrupted a GPM granule and produced two partial `SCENE_PIPELINE` COMPLETED events for dataset 31/32 (2026-09-21) before this existed. The lock releases itself if its holding process dies in any way (the kernel guarantees this; a PID file does not, since a dead PID is indistinguishable from a live one without guessing, and PIDs are reused).

**`etl/atomic_write.py`** — `atomic_path()` writes to a process+thread-unique temp file and `os.replace()`s it into place, which is atomic on both Windows and POSIX: the final path always holds either the complete old version or the complete new version, never a partial one. Without it, any reader of the final path during a write — the next stage, a second process on the same job, or a later run treating "file exists" as "file is complete" — could read corrupted data with nothing signaling the problem; this is what produced the corrupted granule cache for dataset 31/32. `etl/download_guard.py`'s `adopt_file()` uses the same process+thread-unique-temp-name pattern for its hardlink/copy adoption path.

`PipelineLogManager.completed_aux_dates()` is a related restart-resilience fix: without it, `_ingest_aux_days` re-downloaded and reprocessed the *entire* auxiliary date range from the first date on every server restart (~20 minutes for a 3-month dataset) before the Sentinel-1 queue could even start, and since that phase doesn't move the scene counter, the UI looked frozen. It checks `details.aux_complete = true` specifically, not just `status = COMPLETED`, because COMPLETED only means *some* file was written — a date where MODIS succeeded but GPM failed is still COMPLETED, and skipping it on resume would make the gap permanent instead of letting the next run patch it.

---

## Implementation Notes

Breadcrumbs for whoever touches the per-satellite processing model next. This
section records **decisions that are not obvious from the code**, the traps that
are already paid for, and the limits that are deliberate rather than accidental.

The rest of this file describes what the pipeline does. This section describes why it does
it that way, and what will bite you if you change it.

### 1. There is exactly one place that decides levels

`etl/processing_plan.py` is the only module allowed to answer:

| Question | Function |
|---|---|
| Which sources run, at which levels? | `load_processing_plan()` → `ProcessingPlan` |
| How many cross-source outputs (fusion files, preview folders)? | `ProcessingPlan.output_levels()` |
| Within one run, what level does source X contribute at? | `SourcePlan.level_for_run(run_level)` |
| Which tier do I read for that? | `SourcePlan.tier_for_run(run_level)` |
| How do I tag `data_products.processing_level`? | `SourcePlan.level_for_tier(tier)` |

Fusion (module 9), preview (module 10) and the orchestrator (module 5) all call
these rather than re-deriving the rules. That is the point: before this module
existed, each module had its own reading of "what RAW means", and those readings
drifted the moment one of them changed. If you need a new rule, add it here, not
at the call site.

`processing_plan.py` deliberately imports nothing from other ETL modules so it
can be imported from anywhere without a cycle. Keep it that way.

#### The `output_levels()` rule, in one sentence

Two runs if any single source was configured `{RAW, PROCESSED}`; otherwise one.

The tempting alternative — "one run per distinct level across all sources" —
looks more general and is wrong. For `s1[RAW] + modis[PROCESSED]` it produces two
stacks whose contents are byte-identical, because neither source has a second
level to contribute. Twice the disk, no extra information.

### 2. Fusion is anchored to Sentinel-1, on purpose

`create_fusion_stack()` raises if `SENTINEL1` is not configured.

The S1 scene provides two things nothing else in the pipeline provides:

1. **The reference grid.** Every other layer is reprojected onto the S1 raster's
   CRS, transform and shape. MODIS (250 m sinusoidal) and GPM (~11 km) are both
   far coarser; picking either as the reference would throw away the resolution
   that makes the stack useful.
2. **The fusion dates.** The orchestrator drives everything per S1 scene
   (`_process_scene`), so "which dates exist" is literally the set of S1
   acquisitions. MODIS and GPM are daily and would otherwise fuse every day in
   the range.

"Fusion Stage" above mentions "or largest-extent source if no S1" as a design intent.
That path is **not implemented**. Implementing it means more than picking a grid:
you also need a date driver for a pipeline that currently has no scene loop for
non-S1 sources. Do not add a half version that silently produces stacks on an
arbitrary grid.

### 3. Traps already paid for

These were real failures found by `tests/test_per_satellite_config.py`. Each has
a regression test; don't "simplify" them away.

#### 3.1 `band_name` carries the level for FUSION products

`data_products` de-duplicates via `is_latest` on
`(scene_id, band_name, product_tier, dataset_id)`. A RAW+PROCESSED dataset writes
two stacks for the same date, same scene, same tier. With a shared `band_name`
of `FUSION`, registering the second marks the first `is_latest = False` — the RAW
stack still exists on disk but disappears from every API listing.

Hence `FUSION_RAW` / `FUSION_PROCESSED`, and migration 018 widening
`data_products.band_name` to `VARCHAR(20)` (`FUSION_PROCESSED` is 16 characters;
at `VARCHAR(10)` the INSERT failed *after* the HDF5 was already on disk, so the
stage failed leaving an orphan file).

`quality_metrics.band_name` stays `VARCHAR(10)` — it only ever holds `VV`/`VH`.

#### 3.2 `fusion_products` unique key includes `processing_level`

Migration 018 replaces `uq_fusion_date_region` with
`uq_fusion_date_region_level`. Same reason: without it the second stack of a
date overwrites the first row, discarding exactly the comparison the dataset was
configured to produce.

Migration 021 then replaces it with `uq_fusion_dataset_date_level`
(`dataset_id, feature_date, processing_level`) and adds
`fusion_products.dataset_id`. The region-based key still let two *datasets* over
the same AOI and date share one row: try1/try2/try3 (HYBRID / FULL_COVERAGE /
CO_OCCURRENCE, same Tangerang AOI) all wrote `fusion_id=43`, and whichever
finished last overwrote the other two's path and strategy. `region_id` left the
key because a shared S1 scene carries the region of the dataset that first
registered it, so a single dataset can legitimately hold rows with two
region_ids.

#### 3.2b Fusion stacks on borrowed / missing S1 days

- A FULL_COVERAGE day that **borrows** an S1 scene (offset ≥ 1) registers its
  `data_products` row on a per-date `NASA_AUX_FUSION_*` placeholder scene, not on
  the borrowed scene. The `is_latest` dedup key is
  `(scene_id, band_name, tier, dataset_id)`, so days sharing one scene marked each
  other stale and only the last stack stayed visible. `fusion_products.s1_scene_id`
  still records the scene actually used.
- A day with **no** S1 in tolerance uses the grid of any S1 raster the dataset
  already has, falling back to the AOI formula only when there is none. The AOI
  formula does not match module1b's reprojected grid (try2: 578×473 vs 571×468),
  which made the daily series unstackable.
- Every job that reaches `start_job` is closed on the failure path too
  (`MetadataManager.fail_open_jobs`, plus explicit FAILED closes in module9).
  Previously any exception between `start_job` and `complete_job` left the job
  RUNNING forever.

#### 3.3 Date matching must be done in UTC

`acquisition_datetime` is `TIMESTAMPTZ`; psycopg2 returns it in the **database
session's** timezone. MODIS/GPM files are keyed by UTC date (the orchestrator
uses `acq_date.date()` from the download result, which is UTC).

In Asia/Jakarta, a 22:50 UTC acquisition comes back as `05:50+07:00` the *next*
day. Deriving candidate dates from that value shifts the whole search one day and
every auxiliary layer fills with NaN — with no error anywhere, because "no file
found" is a supported condition. `_as_utc()` in `module9_fusion.py` normalizes
first.

#### 3.4 Auxiliary data comes from the feature date, never the day after

`_find_aux_daily_file()` looks for the MODIS/GPM file of the fusion's feature
date D, then D-1 as a fallback (`AUX_DAY_OFFSETS = (0, -1)`). It never uses D+1.

This replaced a "nearest midnight within 24 h" rule. For the descending S1 pass
over Jabodetabek (~22:25 UTC) the nearest midnight is the *next* day's, so
dataset 24_try8 wrote `fusion_20250114_*.h5` containing GPM and MODIS from 15
Jan. An IMERG daily granule covers 00:00-24:00 UTC, so every millimetre in that
"24h" layer fell 1.6-25.6 h *after* the image was taken: future information
leaking into a predictor. The old rule was also non-deterministic: D+1 was only
picked when it happened to be on disk (HYBRID/FULL_COVERAGE download daily,
CO_OCCURRENCE does not).

The reference is the feature date, not the scene's acquisition time. On days
that borrow an S1 scene from another date (FULL_COVERAGE), the aux layers must
still belong to that day.

A daily product cannot be cut at the acquisition time, so the same-day window
still includes some rain after the image (up to ~13 h for the 11:15 UTC
ascending pass). Each GPM layer records this as `hours_after_s1_acquisition`
(from the `WINDOW_END_UTC` tag written by module8), so a consumer can filter
or pick a window that stays before the acquisition.

#### 3.5 Unconfigured sources get no group; configured-but-missing get NaN

`fusion_layers_for()` builds the layer list from the configured sources only.
A consumer must be able to distinguish "this sensor was never requested" from
"requested, no data that day" — a NaN-filled group for an unrequested sensor
erases that difference and quietly teaches a model that the sensor is always
absent.

### 4. Naming conventions worth knowing

| Thing | Pattern | Defined in |
|---|---|---|
| Fusion HDF5 | `fusion_{YYYYMMDD}_{level}.h5` | `module9_fusion.fusion_h5_name()` |
| Fusion sidecar | `fusion_{YYYYMMDD}_{strategy}_{level}_metadata.json` (same stem as the HDF5) | `module9_fusion.fusion_metadata_name()` |
| MODIS raster | `modis_{YYYYMMDD}_{band}.tif` | `module7_modis_download.band_filename()` |
| GPM raster | `gpm_rain_{window}_{YYYYMMDD}.tif` | `module8_gpm_download.band_filename()` |
| S1 BRONZE | `*_{BAND}_crop.tif` | `module2_crop.run()` |
| S1 SILVER/GOLD | `*_{BAND}_lee.tif` | `module3_lee_filter.run()` |

Never hardcode these at a call site — module 9 and module 10 both look files up
through the defining function so a rename stays a one-line change.

**GPM RAW is the one place where the on-disk name and the HDF5 name differ on
purpose.** The file is `gpm_rain_24h_*.tif` (the pipeline's window key), but the
HDF5 layer is `/gpm/rainfall_daily`. The layer name is honest about content —
one day's rainfall, no accumulation — and stops a consumer from assuming a RAW
stack's "24h" is comparable to a PROCESSED stack's genuinely accumulated 24h
window. `_AuxLayer.name` vs `_AuxLayer.file_key` encodes this split.

### 5. Preview levels mirror fusion levels

Preview renders once per `output_levels()` entry, so preview folders and fusion
files always come in matching sets. If you change the level rule, both follow
automatically — that is why they share `output_levels()` instead of each having
its own logic.

The level is a **path segment** (`preview/{date}/{LEVEL}/{kind}/`) because both
levels emit identical filenames (`s1_vv.png`). The composite lives in its own
`composite/` folder rather than in `colored/` because its sidecar schema is
genuinely different: channel mapping instead of colormap + value range.

`preview_options = []` means "the user wants no previews" and `NULL` used to mean
"unspecified". Migration 018 backfills NULL to all three variants and sets the
column `NOT NULL`, so the empty array is now unambiguous — the orchestrator can
skip the whole stage on it without guessing.

### 6. Changed signatures (if you are updating a caller)

| Function | Was | Now |
|---|---|---|
| `module9_fusion.create_fusion_stack()` | returned `fusion_id: int` | returns `list[FusionRun]`, one per run level; takes `plan=` and `fusion_strategy=` |
| `module9_fusion._find_s1_gold()` | GOLD only | `_find_s1_products(..., tier=...)` |
| `module10.resolve_gold_inputs()` | GOLD only | kept as a PROCESSED-level alias of `resolve_source_inputs(..., processing_level=...)` |
| `module10.generate_previews()` | — | added `processing_level=`, `s1_files=`, `options=`; `s1_gold_files=` still accepted |
| `folder_manager.get_preview_kind_dir()` | `(id, name, scene, kind)` | added trailing `processing_level=` (defaults to `PROCESSED`) |

### 7. Two processes can work the same job under `--reload`

`uvicorn --reload` starts a new process while the old one is still shutting down. The new process's `recover_interrupted_jobs()` doesn't know that — it sees a job that looks interrupted and resumes it, while the old process may still be mid-write on the same files. This produced a corrupted GPM granule and two partial `SCENE_PIPELINE` COMPLETED events for the same date (dataset 31/32, 2026-09-21).

`dataset_manager`'s `_active_threads` registry only guards against two *threads in the same process* picking up the same job; it is blind to a second process. `etl/job_lock.py` closes the actual gap with an OS-level file lock per `job_id` (`msvcrt.locking` / `fcntl.flock`) that releases itself if its holding process dies for any reason — a PID file was considered and rejected, because a PID left behind by a killed process is indistinguishable from a live one without guessing, and OSes reuse PIDs.

Companion fix: `etl/atomic_write.py`'s `atomic_path()` (temp file + `os.replace()`) and `download_guard.adopt_file()`'s process+thread-unique temp names close the write-visibility half of the same class of bug — a reader must never be able to observe a half-written file, whether the second writer is a second process or just a retried attempt in the same one. See "Concurrency & Durability Safeguards" above for the mechanics.

**If you add a new module that writes an output file that another stage or process might read while it's being written, route it through `atomic_path()` — do not open a file for writing directly at its final path.**

### 8. Merge and refusion reuse the orchestrator, they don't reimplement it

`etl/dataset_merge.py` (stitches FUSION-tier HDF5s across sibling datasets — DECISIONS.md D19) and `etl/refusion.py` (rebuilds one date's PREVIEW+FUSION from rasters already on disk, no re-download — DECISIONS.md D21) both exist because the normal per-scene pipeline (`run_dataset_job`) has no path back into `_finalize_date` for a date that's already `scene_is_done`.

`refusion.refuse_date()` deliberately reconstructs `_JobContext`/`_SceneResult` and calls `module5_orchestrator._finalize_date()` rather than calling `module9_fusion.create_fusion_stack()` directly. A date can be covered by more than one S1 frame (the `s1_mosaic` case); calling `create_fusion_stack` with a single `scene_id` reproduces exactly the "later frame silently overwrites the earlier one" bug `s1_mosaic` was built to fix. If you're tempted to write a faster path that skips `_finalize_date`, that's the trap: mosaic → preview → fusion → reference layers is one sequence living in one function, and a second copy of it will drift the moment one of the two is changed.

`dataset_merge.merge_stacks()` refuses (`GridMismatch`) rather than resampling when input grids don't align to within `ALIGN_TOL_PX` (0.01 px). This is not a conservative default that could be loosened later — Sentinel-1 backscatter is stored as linear sigma0 in these stacks, and interpolating linear-scale SAR values is misleading precisely at the land/water boundary where flood signal lives. Don't add a resampling fallback here; a rejected merge is a correct outcome when grids don't line up bit-for-bit on a pinned `fusion_grid`.

### 9. Running the tests

```
pytest tests/ -v --cov=etl --cov=api
```

`tests/test_per_satellite_config.py` is the end-to-end one: it stubs only the
network layer and the per-pixel math, writes **real** GeoTIFFs, and then opens
the resulting HDF5. `tests/test_pipeline_branching.py` covers tier/level tagging
with cheap byte placeholders and never reaches fusion.

If you add a stub that writes placeholder bytes where fusion will read, fusion
will fail in `rasterio.open` — that is the tests telling you the stub is in the
wrong layer, not a bug in module 9.

A note for debugging on Windows: pytest's default temp dir plus this project's
long dataset folder names sits close to the 260-character `MAX_PATH` limit.
Passing a deep `--basetemp` can produce `FileNotFoundError` on file writes that
have nothing to do with the code under test.
