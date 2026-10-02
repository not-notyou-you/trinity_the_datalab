# Interface

The two ways a consumer touches this system: the REST API and the web dashboard. (Merged from the former `API.md` + `INTERFACE.md` — see DOCS/README.md for the current doc set.)

## REST API

**Base URL**: `http://localhost:8000/api/`
**Format**: JSON. **Auth**: None (add before public exposure). **CORS**: `*` (restrict before production).
**Interactive docs**: `/docs` (FastAPI/Swagger). Non-API routes: `/` serves the landing page (`web/index.html`), `/app` serves the application (`web/app.html`); everything else under `/` is static files from `web/`.

### Health

```
GET /api/health
→ 200 { "status": "healthy", "db_connected": true, "pool": {...}, "timestamp": "..." }
```

### Datasets

#### Create Dataset
```
POST /api/datasets
```
```json
{
  "region_id": 1,
  "date_start": "2024-01-01",
  "date_end": "2024-01-31",
  "name": "Ablation Study Jan 2024",
  "description": "Optional free text",
  "sources": {
    "sentinel1": { "processing": ["RAW", "PROCESSED"] },
    "modis": { "processing": ["PROCESSED"] },
    "gpm": { "processing": ["RAW"] }
  },
  "fusion_strategy": "HYBRID",
  "fusion_output_only": false,
  "s1_match_tolerance_days": 2,
  "preview_options": ["COLORED", "COMPOSITE"],
  "quality_settings": { "min_cloud_cover": null, "min_quality_score": null, "resolution_m": null, "orbit_direction": null },
  "generate_preview": true
}
```

**`sources` object**:
- Keys: `sentinel1`, `modis`, `gpm`. Include only sources to ingest (omitted = not ingested).
- `processing`: array of 1–2 values from `["RAW", "PROCESSED"]`. Required per included source.
- At least 1 source must be included.
- What RAW/PROCESSED means differs per satellite (see DOCS/PIPELINE.md).

**Fields**:
- `region_id` (int, optional): the wizard's actual path — a `regions_of_interest` row picked from the map/list. This is what the UI sends.
- `location` (string, optional): preset name, free-text (geocoded), or `"lat1,lon1,lat2,lon2"` bbox string — kept for older/CLI callers, resolved by name then geocoding. Exactly one of `region_id`/`location` must be given.
- `description` (string, optional).
- `quality_settings` (object, optional): `{min_cloud_cover, min_quality_score, resolution_m, orbit_direction}`, all optional. `orbit_direction` is `"ASCENDING"`, `"DESCENDING"` or null (both); Sentinel-1 scenes of the other direction are dropped after discovery (DOCS/PIPELINE.md, Stage 1).
- `generate_preview` (bool, default `true`): whether the PREVIEW stage runs at all — separate from `preview_options`, which controls *which* PNG kinds it renders when it does.

**Validation**:
- `sources`: At least 1 key required. Each key must have non-empty `processing` array.
- `fusion_strategy`: Required if `sources` has >1 key, must be null/omitted if only 1 key. Values: `CO_OCCURRENCE`, `FULL_COVERAGE`, `HYBRID`. See DOCS/PIPELINE.md "Strategies: two axes, not one" — the strategy controls **both** which aux dates are downloaded and which dates become an HDF5.
- `fusion_output_only` (bool, default `false`): keep only the fusion HDF5s; per-satellite artifacts are deleted **after** each date's stack is written. Not a "skip processing" switch — fusion reads those artifacts, so they are still built. Rejected with 400 if no `fusion_strategy` is set, since that would leave the dataset empty.
- `s1_match_tolerance_days` (int 0–14, default `2`): `FULL_COVERAGE` only. How far it may borrow a Sentinel-1 scene from a neighbouring date. `0` means same-day only. Days with no scene in range still produce a file, with the `sentinel1/` group filled with NaN; the gap actually used is recorded in `fusion_products.s1_offset_days` (NULL when there was no scene at all).
- `preview_options`: Optional. Values: `GRAYSCALE`, `COLORED`, `COMPOSITE`.
- `date_end >= date_start`.

```
→ 201 {
    "dataset_id": 7,
    "job_id": 42,
    "status": "QUEUED",
    "source_configs": [
      { "source": "sentinel1", "processing": ["RAW", "PROCESSED"] },
      { "source": "modis", "processing": ["PROCESSED"] },
      { "source": "gpm", "processing": ["RAW"] }
    ]
  }
→ 400 { "detail": "fusion_strategy required when multiple sources configured" }
→ 400 { "detail": "sources.modis.processing must contain at least one value" }
```
`job_id` is an integer (`dataset_jobs.job_id`), not a UUID. The job starts immediately only if fewer than `MAX_ACTIVE_JOBS` (default 2) dataset jobs are running; otherwise it stays `QUEUED` in a FIFO queue.

#### List Datasets
```
GET /api/datasets?limit=10&offset=0
→ 200 { "items": [...], "total": 42, "offset": 0, "limit": 10 }
```
**Key is `items`, not `datasets`** (`DatasetManager.list_datasets`'s actual return shape) — a stale doc/consumer that reads `datasets` gets `undefined` silently rather than an error and just renders an empty list (fixed in the merge panel after shipping exactly this bug — commit `ae19569`). Only `dataset_kind=STANDARD` datasets are listed here; the legacy LIVE dataset is reached through `/api/live`, and Live Area datasets (`LIVE_AREA`) through `/api/live/areas`. The per-dataset endpoints below (`/report`, `/storage/*`, `/preview`, …) do work on a Live Area's `dataset_id`. Each item includes: id, name, region, dates, status, source_configs (array), fusion_strategy, `scenes_by_source`/`bytes_by_source` (one aggregate query for the whole page, not per-card), total_size_bytes.

#### Get Dataset Detail
```
GET /api/datasets/{id}
→ 200 { everything List Datasets' item has, plus bbox_wkt, region_id, quality_settings,
        live_last_checked_at, deleted_at }
```
Not a scene/product listing — those live under `GET /api/scenes?dataset_id=` and `GET /api/products?dataset_id=` respectively (`DatasetDetail` extends the same `DatasetItem` the list endpoint returns, it does not embed either list).

#### Dataset Status
```
GET /api/datasets/{id}/status
→ 200 { "dataset_id": 7, "job_id": 42, "status": "PROCESSING", "total_scenes", "downloaded_count",
        "processed_count", "failed_count", "cleaned_count", "progress_percent": 45, "paused", "pause_reason",
        "scenes": [...], "layers": [ { "key", "source", "phase", "ratio" } ] }
```
`layers` feeds the progress ring on the dataset card (one arc per source × phase: download / processing / fusion).

Fields for the card's progress bar (computed by `DatasetManager.get_progress()`):
- `queue_position` (int | null) — position in the `MAX_ACTIVE_JOBS` FIFO queue, 1 = next; null when not queued.
- `timing` (object | null) — `{ elapsed_s, idle_s, stalled, last_activity_at }`; `stalled` is true after `PROGRESS_STALL_AFTER_S` without a received byte or log event. Null once the job has finished.
- `waiting` (object | null) — the server wait in progress (e.g. 429 `Retry-After`, backoff): `{ source, reason, ..., remaining_s }`.
- `alerts` (array) — `[{ source, message }]`, e.g. an expired `NASA_EARTHDATA_TOKEN`.

These four fields used to be computed but missing from the `DatasetProgressResponse` model, so FastAPI dropped them and the queue position, wait notes, token alert and stall warning never appeared. They are now declared on the model.

#### Pause / Resume / Cancel / Retry
```
POST /api/datasets/{id}/pause    → 200 { "status": "PAUSED" }
POST /api/datasets/{id}/resume   → 200 { "status": "QUEUED", "resume_count": N }
POST /api/datasets/{id}/cancel   → 200 { "status": "CANCELLED", "deleted_files": N, "retained_tier": "COG+FUSED" }
POST /api/pipeline/trigger?dataset_id=7 → 200 { "started": true, "dataset_id": 7, ... }
```
`cancel` deletes intermediate tiers but keeps the analysis-ready COG + FUSED output. Retry lives under `/api/pipeline/trigger`, not under `/api/datasets/{id}`, and only re-runs a dataset whose last job is `FAILED` (400 otherwise).

#### Logs
```
GET /api/datasets/{id}/logs?stage=LEE_FILTER&status=FAILED&scene_id=...&limit=20&order=desc
→ 200 { "total": N, "limit": 20, "logs": [{ "log_id", "timestamp", "stage", "scene_id", "status", "duration_sec", "error_type", "error_message" }] }
```

#### Download
```
GET /api/datasets/{id}/download?tier=cog&source=modis
→ 200 (streaming ZIP)
```
No filters: the whole dataset. `tier` alone: just that tier across all sources. `tier` + `source`: one source at one tier (e.g. only MODIS COG, to avoid pulling tens of GB of RAW). `source` without `tier` is rejected (400). Accepts both the D14 tier names and the legacy medallion names (`?tier=gold` and `?tier=cog` are equivalent — see DOCS/DECISIONS.md D14).

#### metadata.json (as written to disk)
```
GET /api/datasets/{id}/metadata
→ 200 { ...raw contents of data/datasets/{id}_{slug}/metadata.json... }
→ 404 if the first job hasn't completed yet
```
A convenience mirror of what the orchestrator wrote to disk, not a source of truth — if it disagrees with any other endpoint, the database is right.

#### Deletion Progress
```
GET /api/datasets/{id}/deletion-progress
→ 200 { "dataset_id": 7, "status": "...", "files_deleted": N, ... }
→ 404 if no deletion is in progress for this dataset
```

#### Preview Gallery
```
GET /api/datasets/{id}/preview?scene=20250123
→ 200 { "dataset_id", "tier": "preview", "kinds": [...], "processing_levels": [...],
        "scene_count", "total_size_bytes", "scenes": [ { "scene", "acquisition_date",
        "s1_scene_key", "sources_present", "processing_levels", "default_processing_level",
        "by_level": { "RAW": {...}, "PROCESSED": {...} }, "kinds": {...} } ] }
GET /api/datasets/{id}/preview/{scene}/{level}/{kind}/{filename}   → PNG, level = RAW|PROCESSED
GET /api/datasets/{id}/preview/{scene}/{kind}/{filename}           → PNG, level defaults to PROCESSED (falls back to whichever level exists) — kept for old links
```
Always 200 with an empty gallery for a dataset that has no PREVIEW output yet — that is a normal state, not an error. A scene with both RAW and PROCESSED configured for the same source carries **two** levels under `by_level`; top-level `kinds` mirrors whichever level `preferred_preview_level()` picks (PROCESSED first). A scene item may also carry `coverage_quality` / `coverage_min_valid_fraction`, copied from root attributes of that date's `*_processed.h5` if present — no current pipeline stage writes those attributes, so in practice they are absent.

#### Reference Layers (masks)
```
GET /api/datasets/{id}/masks
→ 200 { "dataset_id", "layer_count", "total_size_bytes",
        "applies_to": "semua tanggal dataset ini (grid sama dengan stack fusion)",
        "layers": [ { "key": "land_distance"|"water_occurrence", "label", "interpretation",
                       "image_url", "data_file", "size_bytes", "statistics", "source",
                       "semantics", "caveats" } ] }
GET /api/datasets/{id}/masks/{filename}   → PNG
```
Dataset-scoped, not date-scoped — one land/sea distance raster and one water-occurrence raster serve every date of the dataset (see DOCS/PIPELINE.md "Reference Layers"). Always 200 with an empty list for datasets created before this feature, or that haven't reached FUSION yet.

#### Storage — per dataset
```
GET /api/datasets/{id}/storage/by-source
→ 200 { "dataset_id", "sources": { "sentinel1": { "size_bytes", "stages": [...] }, ... }, "fusion": {...}|null }
```
Per-satellite view for the Detail panel: each configured source broken into `download` (rank 0–1) and `processing` (rank 2–3) stages, each stage listing per-scene size/file counts.

```
GET /api/datasets/{id}/storage/summary
→ 200 { "dataset_id", "legacy_layout": bool, "tiers": { "ALIGNED": {...}, "COG": {...}, ... },
        "sources": { "sentinel1": {...}, ... }, "total_size_bytes", "total_size_mb" }
```
Canonical D14 tier names as keys. `legacy_layout: true` means this dataset predates the D15 relayout (`{YYYYMMDD}/{tier}/{source}/` instead of `{source}/{RAW|PROCESSED}/`) and the File structure panel shows a legacy-layout notice instead of a tree.

```
GET /api/datasets/{id}/storage/files/{tier}?source=modis&scene=20250123
→ 200 { "dataset_id", "tier", "source", "scenes": [ { "scene", "source", "files": [ { "name", "path", "size_mb" } ] } ] }
```
`tier` accepts both vocabularies (D14 canonical or legacy); `source` is rejected with 400 for tiers that have no per-source split (FUSED, PREVIEW). Also surfaces the `_granule_cache/{source}/` loose files (raw NASA granules shared across dates) under a synthetic `scene` label, since they live outside any date folder.

#### Delete
```
DELETE /api/datasets/{id}?force=false
→ 200 { "status": "DELETING", "dataset_id": 7 }
```
`force=true` stops a running job before deleting; otherwise a dataset mid-job is rejected with 400.

#### Report (PDF / JSON)
```
GET /api/datasets/{id}/report?force=false        → 200 application/pdf, filename {slug}_report.pdf
GET /api/datasets/{id}/report/json?force=false   → 200 application/json, filename {slug}_report.json
→ 404 dataset not found
→ 400 { "detail": "..." }  ReportGenerationError (not enough data, or PDF assembly failed)
```
Synchronous — no job/polling, because the report only reads aggregates and renders small charts (seconds, not hours). The PDF and JSON are written together to `data/datasets/{id}_{slug}/reports/report_{UTC timestamp}.{pdf,json}`; the latest one is reused while it is newer than `datasets.updated_at`. `force=true` regenerates. The JSON endpoint regenerates once if the cached PDF predates the JSON export. Contents: DOCS/REPORT.md.

#### Get Last Configuration (for "Reuse Previous Config")
```
GET /api/datasets/last-config
→ 200 {
    "region_id": 1,
    "region_name": "Jabodetabek",
    "sources": {
      "sentinel1": { "processing": ["RAW", "PROCESSED"] },
      "modis": { "processing": ["PROCESSED"] }
    },
    "fusion_strategy": "HYBRID",
    "fusion_output_only": false,
    "s1_match_tolerance_days": 2,
    "preview_options": ["COLORED", "COMPOSITE"],
    "date_start": "2026-08-01",
    "date_end": "2026-08-31",
    "created_from_dataset_id": 42,
    "created_at": "2026-09-06T14:08:51Z"
  }
→ 404 { "detail": "No dataset found yet" }
```

Returns the most recently created dataset's configuration (region, sources with processing levels, fusion strategy, preview options). Used by the frontend's "Reuse Previous Config" button to pre-populate the creation wizard.

### Scenes

```
GET /api/scenes?region_id=1&orbit_direction=ASCENDING&date_from=2024-01-01&date_to=2024-01-31&only_gold=true&limit=20&offset=0
→ 200 { "total", "limit", "offset", "items": [ { "scene_id", "product_identifier", "platform",
        "orbit_direction", "cloud_cover_percent", "acquisition_datetime", ... } ] }
GET /api/scenes/{scene_id}          → full scene detail (adds raw_file_path, checksum_md5, incidence angles, ...)
GET /api/scenes/{scene_id}/status   → { "scene_id", "stages": [...], "overall_status": "NOT_STARTED"|"IN_PROGRESS"|"COMPLETE"|"FAILED" }
```
`only_gold=true` restricts to scenes that have a COG-tier product (checks both `COG` and legacy `GOLD`). This router is Sentinel-1 only — MODIS/GPM granules are tracked as `nasa_scenes`, not `satellite_scenes`, and are not exposed by `/api/scenes`.

### Products

```
GET /api/products?dataset_id=7&scene_id=...&tier=COG&source=SENTINEL1&band_name=VV&latest_only=true&valid_only=true&limit=20&offset=0
→ 200 { "total", "limit", "offset", "items": [ { "product_id", "scene_id", "product_tier", "source",
        "band_name", "file_name", "file_size_mb", "data_hash_sha256", "crs", "is_valid", "is_latest", ... } ] }
GET /api/products/{id}
GET /api/products/{id}/download   → binary file (404 if the file isn't on local disk — e.g. remote storage)
GET /api/products/{id}/verify     → { "valid": true, "checksum_expected": "...", "checksum_actual": "..." }
```

`tier` accepts either vocabulary — `?tier=COG` and `?tier=GOLD` match the same rows (DOCS/DECISIONS.md D14). `source=SENTINEL1|MODIS|GPM|FUSION`. `latest_only`/`valid_only` default `true`; set `false` to include superseded/invalidated rows (e.g. after a tier cleanup).

### Quality

```
GET /api/quality/{scene_id}
→ 200 { "scene_id", "bands": [{ "band_name": "VV", "quality_flag": "PASS", "quality_score": 82,
        "backscatter_mean_db", "speckle_index", "nodata_pixels", ... }], "overall_quality": "PASS"|"WARNING"|"FAIL" }
→ 404 if module6 (QUALITY_ANALYTICS) hasn't run for this scene yet

GET /api/quality/summary/stats?region_id=1&n_days=30
→ 200 { "period_days", "region_id", "flags": { "PASS": {"count", "avg_score"}, ... } }

GET /api/quality/dataset/{id}/by-source
→ 200 { "dataset_id", "sources": [ { "source": "SENTINEL1", "kind": "RADIOMETRIC", "quality_score",
        "quality_flag", "bands": {"VV": 82.1, "VH": 79.4} },
        { "source": "MODIS", "kind": "COVERAGE", "quality_score", "bands": {"FLOOD": 100.0, "NDVI": 96.7, "NDWI": 96.7} } ] }
```
Sentinel-1 is the only source with a real radiometric QA stage — MODIS/GPM report `kind: "COVERAGE"` instead: what percent of the expected bands (`FLOOD/NDVI/NDWI` for MODIS, `RAIN_24H/RAIN_72H/RAIN_7D` for GPM) actually landed on disk, since speckle index and backscatter stats don't mean anything for rainfall or vegetation indices. Don't compare `RADIOMETRIC` and `COVERAGE` scores directly — the `kind` field exists specifically to keep them from being conflated.

### Lineage

```
GET /api/metadata/lineage/{product_id}?direction=ancestors
→ 200 { "product_id", "direction", "chain": [{ "source_product_id", "target_product_id",
        "transformation_type", "source", "parent_tier", "child_tier", "checksum_source", "checksum_target" }], "total_steps" }
```
`direction=ancestors` (default) walks back to the RAW source; `direction=descendants` walks forward to derived products. Each step carries `source` plus `parent_tier`/`child_tier` so a dataset's parallel per-sensor chains (S1 RAW→COG, MODIS/GPM ALIGNED→COG) stay distinguishable. `transformation_type` values include: `CALIBRATE`, `CROP`, `LEE_FILTER`, `QUALITY_ANALYTICS`, `GOLD_EXPORT`, `COMPUTE_NDVI`, `COMPUTE_NDWI`, `ACCUMULATE_RAIN`, `FUSE`.

Note: the old `/api/preview/*` router (thumbnail-on-the-fly from COG) has been **removed** — the gallery reads PREVIEW-tier PNGs through `/api/datasets/{id}/preview` instead, to avoid two different render/stretch definitions of "preview".

### Live Monitoring (Live Areas)

`etl/live_monitor.py`; behaviour in DOCS/PIPELINE.md "Live Monitoring". Responses are plain dicts (no Pydantic response model).

```
GET    /api/live/areas
→ 200 [ { "area_id", "dataset_id", "name", "region_id", "location_label", "bbox_wkt", "retention",
          "enabled", "status", "status_message", "running", "progress", "alerts", "last_result",
          "last_checked_at", "scene_dates", "scene_count", "latest_scene_date", "area_status",
          "total_size_bytes", "created_at" } ]

POST   /api/live/areas   { "region_id": 3, "name": "Padang", "retention": 6 }
→ 201 area dict        (name optional — defaults to the location name; retention 1–12, default 6)
→ 400 more than 5 active areas, unknown region, ...
       The first cycle (backfill) starts immediately in the background.

GET    /api/live/areas/{id}                         → area dict (404 if unknown/deleted)
PATCH  /api/live/areas/{id}   { "name"?, "retention"?, "enabled"? }
→ 200 area dict        lowering retention deletes the excess scenes now; raising it starts a cycle
DELETE /api/live/areas/{id}
→ 200 { "area_id", "status": "DELETED", "freed_bytes", "deleted_scenes" }
       files deleted permanently, live_scenes/live_events kept

POST   /api/live/areas/{id}/check
→ 200 { "area_id", "started": bool, "message": "Cycle started" | "A cycle is already running" }

GET    /api/live/areas/{id}/card?date=YYYY-MM-DD
→ 200 { "area": {...}, "scene": { "date", "status", "source_status", "metrics", "interpretations",
          "area_status", "previews": { key: { ..., "url", "legend", "source_date" } }, "previews_skipped",
          "updated_at" } | null,
        "dates": [ { "date", "status", "level" } ], "forecast": {...}, "forecast_updated_at" }
       date omitted = latest stored scene; deleted scenes never appear in "dates"

GET    /api/live/areas/{id}/preview/{YYYY-MM-DD}/{key}.png    → PNG (Cache-Control: 1 day)
       key ∈ s1_vv, s1_vh, modis_flood, modis_ndvi, modis_ndwi, gpm_rain_24h, gpm_rain_72h, gpm_rain_7d

GET    /api/live/areas/{id}/events?limit=100     → cycle step log (live_events), newest first
GET    /api/live/areas/{id}/activity?limit=5     → latest entries merging live_events with the area
                                                   dataset's processing_logs: { "timestamp", "source",
                                                   "stage", "status", "message", "scene_id", "details" }
GET    /api/live/areas/{id}/log                  → every scene ever stored, INCLUDING deleted ones:
                                                   { "date", "status", "source_status", "metrics",
                                                     "interpretations", "area_status", "created_at",
                                                     "deleted_at", "delete_reason", "deleted_files", "freed_bytes" }
POST   /api/live/areas/{id}/scenes/{YYYY-MM-DD}/retry
→ 200 { "area_id", "scene_date", "started", "message" }   re-fetch failed MODIS/GPM for one scene
```
Dates in Live paths are `YYYY-MM-DD` (400 otherwise), unlike the `YYYYMMDD` keys of dataset previews and merge. `forecast.series` has keys `sentinel1` / `modis` / `gpm`, each `{ label, unit, chart: line|bar, actual: [{date, value}], forecast: {...} }`; `gpm` also carries `thresholds: { alert, high }` (mm, 72 h).

### Legacy Live Dataset

```
GET  /api/live                          → { "dataset_id", "enabled", "status", "required_tiers", "bbox_wkt",
                                              "total_size_bytes", "last_checked_at", "sources": [...] }  (404 if no live dataset yet)
POST /api/live/toggle   { "enabled": true } → { "status": "..." }
POST /api/live/clear                    → { "status": "CLEARED", "freed_bytes", "deleted_count" }
POST /api/live/backfill { "date_start", "date_end" } → { "status", "job_id", "date_range" }
GET  /api/live/scenes?limit=50          → [ { "product_id", "scene_date", "tier", "size_mb" } ]
```
The single `dataset_kind="LIVE"` dataset (D23). Kept for compatibility only: it is no longer scheduled (the daily 02:00 cron was removed when Live Areas replaced it) and the web UI no longer calls these endpoints.

### Regions

```
GET  /api/regions?q=jabo&include_deleted=false&limit=200&offset=0
→ 200 { "items": [{ "region_id", "region_code", "name", "bbox": [minlon,minlat,maxlon,maxlat], "area_km2", "source", "deletable" }], "total" }
GET  /api/regions/geocode?q=Bandung&limit=5&country=id   → { "items": [{ "name", "lat", "lon", "bbox", ... }] }  (Nominatim/OpenStreetMap)
POST /api/regions                                        → 201, creates a USER region from a bbox
PATCH  /api/regions/{id}                                 → rename/re-describe a USER region (403 on SEEDER regions)
DELETE /api/regions/{id}                                 → soft-delete (403 on SEEDER regions; datasets pointing at it stay valid)
POST /api/regions/{id}/restore                           → undo a soft-delete
```
`source` is `SEEDER` (built-in, read-only) or `USER` (created via this API, editable/deletable). `deletable` in the response reflects that.

### Storage — machine-wide

```
GET  /api/storage/summary                       → per-tier disk usage across ALL datasets (legacy tier vocabulary: raw/bronze/silver/gold/preview/fusion), plus `by_source` and `partial_downloads` (.part files)
GET  /api/storage/files/{tier}                   → file listing for one tier, all datasets
POST /api/storage/cleanup   {"tier": "...", "dry_run": false}  → delete every file in a tier across all datasets
POST /api/storage/cleanup/partial?dry_run=false  → delete orphaned .part files (don't run this while a download is in progress)
```
This is the **machine-wide** view (one dataset folder listing scanned across all of `data/datasets/`); `/api/datasets/{id}/storage/*` above is the per-dataset equivalent and uses the current D14 tier vocabulary in its response. `tier="all"` for cleanup deletes only derived tiers (`raw`, `bronze`, `silver`) — `gold` and `fusion` are final deliverables and require naming the tier explicitly.

### Pipeline

```
GET  /api/pipeline/status/current       → stage status for the most recently touched scene (home page progress rail)
POST /api/pipeline/trigger?dataset_id=7 → re-run a dataset whose last job is FAILED (400 otherwise)
```

### Merge (cross-dataset stitching)

`etl/dataset_merge.py`, DOCS/PIPELINE.md "Cross-Dataset Merge", DOCS/DECISIONS.md D19.

```
GET /api/merge/candidates
→ 200 { "candidates": [ { "date", "dataset_ids": [27,28], "dataset_names": [...], "mergeable": bool,
        "blocked_reason": "..."|null, "stack_count", "layers": [...], "output_shape": [h, w],
        "input_bytes", "warnings": [...], "output_name", "already_merged", "output_size_bytes",
        "preview_images": [...] } ], "explanation": "..." }
```
Always 200, including with zero candidates — that's a normal state (no split AOI in this deployment, or nothing has finished fusing yet), not an error. The API itself includes blocked candidates (grid mismatch, etc.) with their `blocked_reason` rather than hiding them — but the web UI currently filters them out client-side before rendering (see "Merge Panel" below); only a consumer calling this endpoint directly sees blocked rows today.

```
POST /api/merge/run   {"date": "20251204", "dataset_ids": [27, 28], "overwrite": false}
→ 200 { "status": "MERGED", "date", "dataset_ids", "dataset_names", "output_path", "output_name",
        "output_shape": [h, w], "output_size_bytes", "input_size_bytes", "layers": [...],
        "preview_images": [...], "warnings": [...], "sources_untouched": true }
→ 400 if fewer than 2 dataset_ids, a stack is missing for that date, or the grids don't line up (`GridMismatch`)
→ 409 if the merged output already exists and overwrite is not true
```
`dataset_ids` must be given explicitly even though the candidate list already implies them — the candidate set can change between when the UI displayed it and when the button is clicked, and naming the ids makes the merge act on exactly what the user saw. Source datasets are never modified or deleted.

```
GET  /api/merge/preview/{date_key}                    → { "date", "images": [...] }
POST /api/merge/preview/{date_key}/rebuild             → re-render PNGs from an existing merged HDF5 without re-merging
GET  /api/merge/preview/{date_key}/{filename}          → PNG
```

```
DELETE /api/merge/result/{date_key}?preview_only=false
→ 200 { "status": "DELETED", "date", "removed": [...], "freed_bytes", "sources_untouched": true }
→ 404 if there is no merged output for that date
```
Deletes only the derivative in `data/merged/` — the source FUSION stacks in each contributing dataset are never touched, which is what makes deleting a merge result safe to free disk (unlike deleting a dataset). `preview_only=true` removes just the PNG folder and keeps the HDF5, to force a from-scratch preview re-render.

### Error Response Format

Routes raise `HTTPException`, which FastAPI serializes as:
```json
{ "detail": "Human-readable message (English)" }
```
All route messages were switched from Bahasa Indonesia to English; code comments remain mostly Indonesian.
There is no `code`/`details` envelope — `error`/`code` fields described in earlier drafts of this document were never implemented; `web/app.js` reads `detail` directly. Two endpoints have custom handlers that keep the same `detail` key: `422` (Pydantic validation errors are flattened from FastAPI's default list-of-objects into one string, joined `"field: message | field2: message2"`) and the catch-all `500` (`{"detail": "Internal server error", "path": "..."}`, logged server-side with the full traceback).

| HTTP | Typical cause |
|---|---|
| 400 | Invalid request body/query (e.g. bad tier name, `fusion_strategy` required, fewer than 2 `dataset_ids` for a merge, bad date format, 6th Live Area, report generation failed) |
| 403 | Attempted write on a `SEEDER` region |
| 404 | Resource doesn't exist, or exists but the requested sub-resource hasn't been produced yet (e.g. quality metrics before module6 ran) |
| 409 | Duplicate name, or overwriting existing output without `overwrite=true` |
| 422 | Pydantic request validation failure |
| 500 | Unhandled server error |
| 503 | Database connection failed |

---

## Web UI

### Technology & Pages

Vanilla HTML5 + CSS3 + JavaScript. Leaflet.js for maps. No build step, no framework. Served as static files by FastAPI. Fonts: IBM Plex Mono (headings, labels, monospace data) + IBM Plex Sans (body text).

| URL | File | What it is |
|---|---|---|
| `/` | `web/index.html` + `web/landing.css` | Landing page "Trinity: The Monitor — Three satellites. One dataset.": short pitch, buttons to the three app views, one card per satellite (Sentinel-1 radar 10 m ~7–8 days; MODIS optical 250 m daily; GPM rainfall ~10 km daily), footer link to `/docs`. Static, dimmed background map; polls `/api/health` for the status dot. |
| `/app` (also `/app.html`) | `web/app.html` + `web/app.js` + `web/icons.js` | The application. Views are selected by hash: `#create`, `#datasets`, `#live`. |

Both pages share `web/style.css` and the same floating navbar.

### Visual Design System

#### Color Palette

```css
--ink: #0A0E1A;          /* deepest background */
--panel: #121A2B;
--panel-alt: #182238;     /* input fields, alternate surfaces */
--hairline: #232F49;      /* borders, dividers */
--text: #E7ECF5;          /* primary text */
--text-dim: #A3B1C9;      /* labels, secondary text */
--cyan: #00F0FF;          /* accent: active states, status, links */
--amber: #F0A63C;         /* warning badges */
--coral: #EF6461;         /* danger/error badges, delete buttons */
```
Per-source colours used in rings/charts: Sentinel-1 `#5B8DEF`, MODIS `#2FA07E`, GPM `#C4762E`.

#### Glassmorphism Surface

`.glass, .floatnav, .panel, .card, .modal, .toast, .region-card, …` share one stack: `background: var(--glass-bg)` (`rgba(10,14,26,0.46)`), `backdrop-filter: blur(22px) saturate(140%)`, inset light/dark shadows, and a gradient border drawn by a masked `::before` (`--glass-grad`: cyan top-left fading to near-black at 65%). Hover adds a cyan glow ring.

#### Border Radius

- Panels, side sections: `18px`
- Cards, modals, inputs, map gap: `10px` (`--radius`)
- Pills, badges, chips: `999px`

#### Background

The app background is a **full-viewport Leaflet map** (Esri World Street Map tiles, `position: fixed; z-index: -2`) with a radial-gradient overlay. Outside Create Dataset the overlay darkens further so cards stay readable.

### Floating Navbar (Expanding Pill)

A compact 56px pill showing only the logo; expands when the pointer enters the top 1/6 of the viewport (or on focus/click):

```
┌────────────────────────────────────────────────────────────────────────────────┐
│ LOGO  THE TRINITY     Home │ Create Dataset │ Dataset Catalog │ Live Monitoring  ● Connected │
│       SENTINEL/MODIS/GPM                                                       │
└────────────────────────────────────────────────────────────────────────────────┘
```

- Active tab: black text on cyan fill. "Home" is a link back to `/`.
- Status dot from `/api/health` every 15 s: `.ok` cyan ("Connected"), `.degraded` amber ("Database issue"), `.down` coral ("Disconnected").

### View 1: Create Dataset (`#create`)

#### Layout: Three-Column Grid

```
┌────────────┐  ┌──────────────────┐  ┌────────────┐
│ Locations  │  │                  │  │ Wizard     │
│ [+ Add     │  │   map window     │  │ 1 Region   │
│  Location] │  │   (transparent   │  │ 2 Satellites│
│ [search…]  │  │    hole onto the │  │ 3 Fusion   │
│ region     │  │    Leaflet map,  │  │ 4 Review   │
│ cards      │  │    cyan selection│  │            │
│            │  │    box, −/+ zoom)│  │ [Next]     │
└────────────┘  └──────────────────┘  └────────────┘
```

Side panels scroll internally. The map gap is a hole in the layout; an SVG mask (`#mapMask`) blocks pointer events outside it so the map can only be dragged inside the window. A hint chip above it shows the selected bbox. Region cards are labelled `system` (SEEDER), `custom` (USER) or `from search`; USER cards have a delete button.

#### Reuse Previous Config

Above the wizard, a **"Reuse Previous Config"** button appears only if a dataset has been created before (a `localStorage` cache decides visibility before the network answers; the values applied always come from `GET /api/datasets/last-config` — D13). A preview lists location, dates, `S1[RAW+PROC] · MODIS[PROC] · GPM[RAW]` and strategy. Clicking fills every wizard field; all remain editable. On API failure an inline error appears and the user can continue manually.

#### Wizard (right panel, step pips 1–4)

**Step 1 — Region & Dates.** Region comes from the left panel. Start/end date. A collapsible advanced block: max cloud cover (%), minimum quality score (0–100), resolution (m), and **Sentinel-1 orbit direction** (Both / Ascending only / Descending only).

**Step 2 — Satellite Data Sources.** "Select All" master toggle, then one card per satellite with its own RAW / PROCESSED checkboxes:

| Source | Card text | RAW | PROCESSED |
|---|---|---|---|
| Sentinel-1 SAR (ESA) | radar, sees through clouds, ~10 m, revisit ~7-8 days | calibration + crop (no Lee filter, no QA) | + Lee filter 7x7 + QA analytics + COG |
| MODIS Optical (NASA) | flood/vegetation, 250 m, daily | flood map only (no derived indices) | + compute NDVI + NDWI from reflectance |
| GPM IMERG Rainfall (NASA/JAXA) | precipitation, ~10 km, daily | daily precipitation (that day only) | + 24 h / 72 h / 7-day accumulation |

Unchecking a source collapses its levels; at least one source with at least one level is required.

**Step 3 — Fusion & Preview.** Fusion strategy (only if >1 source): `CO-OCCURRENCE` (only dates where every source has data), `FULL COVERAGE` (every day; MODIS/GPM downloaded daily, much larger download), `HYBRID` (download daily, assemble per Sentinel-1 date). For FULL_COVERAGE only: "Sentinel-1 pairing tolerance" 0–14 days (default 2). "Keep fusion output only" checkbox. Then optional preview options: GRAYSCALE (2–98 percentile stretch), COLORED (per-source colormap), COMPOSITE (false-color RGB, Sentinel-1 only).

**Step 4 — Review.** Dataset name (required), description (optional), a summary of everything chosen, **Create Dataset**.

Validation runs per step (Next is blocked with an inline error): region selected, start ≤ end, ≥1 source with ≥1 level, strategy required iff >1 source.

### View 2: Dataset Catalog (`#datasets`)

Polls every 10 s (`/api/datasets` + `/status` and the last 5 `/logs` of active datasets). Header has **Refresh**.

#### Dataset Card

```
┌──────────────────────────────────────────────────────────────────┐
│ (ring)  Dataset Name                         [Processing]   [˅]  │
│         Region · 2025-01-01 - 2025-03-31                         │
│         ▓▓▓▓▓▓▓░░░ Processing · 12 ok, 1 failed of 30 · 14 min   │
│         S1[R+P]  MODIS[P]  GPM[P]                                │
├──────────────────────────────────────────────────────────────────┤
│ per-source scenes & bytes · Fusion: HYBRID (fusion output only)  │
│ ring legend                                                      │
│ 12 / 30 scenes done · 1 failed · 4.2 GB size · 1 h 05 duration · ETA │
│ Latest logs (5)                                                  │
│ [Download] [Pause] [Resume] [Retry]  [⋯]                         │
│                                       ├ Details                  │
│                                       ├ Report                   │
│                                       ├ File structure           │
│                                       ├ Preview images           │
│                                       ├ Cancel processing        │
│                                       └ Delete dataset           │
└──────────────────────────────────────────────────────────────────┘
```

- **Progress ring** (`buildRingSVG`) — one arc per `layers` entry from `/status` (source × phase), coloured per source; spins while active.
- **Progress bar** — "Queued (position N)" / "Waiting to start", "Preparing…", "Downloading/Processing · ok/failed/total" with a red share for failures, plus server-wait notes, NASA-token alerts and a "no progress since …" warning.
- **Chips** per satellite: `R` = RAW, `P` = PROCESSED, `R+P` = both. No tier chips — tier is not a user choice (D14).
- **Per-source stats**: `scenes_by_source` / `bytes_by_source` from one aggregate query for the whole page; scenes counted DISTINCT; sources without products are absent ("none yet") rather than zero.
- **Collapse** button hides the body; the collapsed set and open "⋯" menus survive re-render.
- **Actions**: Download (whole dataset ZIP; shown when size > 0), Pause / Resume / Retry by status; the "⋯" menu holds Details, **Report** (opens `/api/datasets/{id}/report` in a new tab), File structure, Preview images, Cancel processing (DOWNLOADING/PROCESSING only), Delete dataset.

#### Details panel
Per-source stages (Downloaded / Processed / Fused) with per-scene status, current stage and last error, from `/storage/by-source` and `/status`.

#### File structure panel
- Storage rows per tier split into per-source segments. **Labels show the drawer, not the tier** (`tierLabel()`): `ALIGNED` → `RAW`, `COG` → `PROCESSED`, `FUSED` → `FUSION`. The download link keeps the real tier name.
- Collapsible source → level → tier tree (native `<details>`); a single-level source collapses that layer.
- File browser per leaf (`/storage/files/{tier}?source=`), grouped by date (read from the filename): Date | Scene | Files | Size.
- Quality per source (`/api/quality/dataset/{id}/by-source`).
- Reference layers (`/masks`) with legend and interpretation.
- Legacy-layout datasets show a notice ("This dataset uses the legacy folder layout …") plus a whole-dataset download link instead of a tree.

#### Preview images panel
Per source and level (e.g. SENTINEL-1 RAW, SENTINEL-1 PROCESSED, MODIS PROCESSED) with a date selector and Grayscale / Colored / Composite groups. Each image card: thumbnail, band label, colormap legend, value range, interpretation note; NoData as checkerboard. Clicking opens a **lightbox** (also used by Live Monitoring) with prev/next, legend and description.

#### Merge Datasets panel (above the list)

Backed by `/api/merge/candidates` and `/api/merge/run` (D19). Shown only when at least one date has stacks in more than one dataset. Fetched only from `loadDatasets()` (opening the view, Refresh, finishing a merge), **not** from the 10 s poll — `candidates` opens every FUSION HDF5 on disk.

```
┌ Merge Datasets ──────────────────────────── 4 dates can be merged  [▾] ┐
│  2025-12-04    JAWA_A · JAWA_B · JAWA_C · JAWA_D                        │
│                14 stacks · 103630 x 32040 px · 8 layers · ~22 GB  [Merge] │
│  2025-12-06    JAWA_A · JAWA_B                                          │
│                Already merged · 5.1 GB         [Re-merge] [Delete]      │
│                [thumbnail] [thumbnail] [thumbnail]                      │
└─────────────────────────────────────────────────────────────────────────┘
```

- `renderMergePanel()` filters candidates to `c.mergeable` — blocked rows (with `blocked_reason`) are visible only to direct API callers.
- **Merge / Re-merge** send exactly the `dataset_ids` shown in that row (`overwrite=true` for Re-merge).
- A merged row without previews shows **Build Preview** (`POST /api/merge/preview/{date}/rebuild`).
- **Delete** (after a confirm) removes only `data/merged/` output, never the source stacks.

### View 3: Live Monitoring (`#live`)

```
┌ Live Area [Padang ▾]  [+ Add Area] ─────────────────────────────────────┐
│ area list (status dot per area)                                          │
│ [Active] 6/6 scenes · 1.4 GB · checked 01/10/2026, 07:02 · message       │
│ Keep [6▾] scenes  [Check now] [Report] [JSON] [Regenerate] [Delete area] │
│ ▓▓▓▓░░ Downloading · 1 ok of 2 · 12 min   (+ latest 5 activity logs)     │
├──────────────────────────────────────────────────────────────────────────┤
│ PADANG · latest scene: 24 Sep 2026                                       │
│ Alert — heavy rainfall, radar-detected wet area is growing               │
│ Sentinel-1 (radar)                       [VV]        [VH]                │
│ MODIS (optical) over Sentinel-1 VH       [Flood] [NDVI] [NDWI]           │
│ GPM (precipitation) over Sentinel-1 VH   [24 h] [72 h] [7 days]          │
│ Stored dates   [24 Sep] [12 Sep] [31 Aug] …   (coloured by level)        │
│ Trends & forecast  [S1 mean VH] [MODIS water area] [GPM 72 h rain]       │
│ Dashed line and band = forecast (method), not observed data.             │
└──────────────────────────────────────────────────────────────────────────┘
```

- **Area selector + list** from `GET /api/live/areas`; **+ Add Area** opens the add modal.
- **Meta row**: status pill (`Backfilling initial scenes`, `Checking for new scenes`, `Active`, `Waiting its turn`, `Error`), `scene_count/retention`, size, last check, status message; retention select (1–12, `PATCH`), **Check now** (`POST …/check`, disabled while running), **Report / JSON / Regenerate** for the area's dataset (once it has a scene), **Delete area** (confirm modal stating the bytes to be freed).
- **Progress** while a cycle runs: phase, ok/failed counts, elapsed time, wait/stall notes, NASA token alert, and the latest 5 entries of `GET …/activity`. After a cycle, a short result line (`last_result`) stays for `LIVE_RESULT_SHOW_S`.
- **Card** (`GET …/card?date=`): header with area status sentence; 8 preview tiles in rows of 2–3–3. A tile shows "nearest DD Mon" when MODIS/GPM came from another date, "not available" when missing, and **Retry download** when that MODIS/GPM source FAILED (`POST …/scenes/{date}/retry`). Clicking a tile opens the lightbox with the legend and the condition sentence (category in bold, coloured by level) — sentences are not shown on the tiles themselves.
- **Stored dates**: newest first; clicking switches the tiles and sentences; deleted scenes never appear.
- **Charts**: three inline SVGs (`lmChartSVG`) — S1 mean VH (line), MODIS NDWI water area % (line), GPM 72 h rain (bars with alert/high threshold lines); dashed forecast with uncertainty band, selected date marked, hover tooltips.
- An area with no processed scene yet shows "… is being prepared" with the progress bar.
- Polls every 10 s only while some area is running, BACKFILLING or WAITING.

### Modals

Glass surfaces, backdrop blur; all close on Escape.
- **Add New Location** (two tabs: *Search by Name* via OpenStreetMap/Nominatim, *Manual Coordinates* with optional pasted `min_lon, min_lat, max_lon, max_lat`)
- **Delete this location?** (soft delete; existing datasets stay accessible)
- **Delete this dataset?** (with "Force-stop any running process")
- **Cancel this dataset?** (Keep Running / Yes, Cancel It)
- **Add Live Area** (location select from saved locations + "Location not listed? Add a new one", optional name, scenes to keep 1–12 default 6, **Save & Start**)
- **Generic confirm** for Live actions (delete area, etc.)

### Toasts

Bottom-right stack, glass surface. Success = cyan tint, error = coral tint. Auto-dismiss after ~4 s.

### UI Language

**English** throughout the UI and API messages (switched from Bahasa Indonesia). Dates are formatted `en-GB`. Dataset status badges show the raw status in sentence case (Queued, Preparing, Downloading, Processing, Paused, Completed, Failed, Cancelled, Deleting).

### Responsive Behavior

- **≤1180px**: narrower side panels (232px), shorter map window
- **≤900px**: single-column stack, map gap hidden, Live grid one column
- **≤560–640px**: preview and Live tile grids drop to 1–2 columns, option rows tighten
- `prefers-reduced-motion`: animations and transitions disabled
