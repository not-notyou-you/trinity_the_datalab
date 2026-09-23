# Interface

The two ways a consumer touches this system: the REST API and the web dashboard. (Merged from the former `API.md` + `INTERFACE.md` — see DOCS/README.md for the current doc set.)

## REST API

**Base URL**: `http://localhost:8000/api/`
**Format**: JSON. **Auth**: None (add before public exposure). **CORS**: `*` (restrict before production).

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
  "quality_settings": { "min_cloud_cover": null, "min_quality_score": null, "resolution_m": null },
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
- `quality_settings` (object, optional): `{min_cloud_cover, min_quality_score, resolution_m}`, all optional.
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
`job_id` is an integer (`dataset_jobs.job_id`), not a UUID.

#### List Datasets
```
GET /api/datasets?limit=10&offset=0
→ 200 { "items": [...], "total": 42, "offset": 0, "limit": 10 }
```
**Key is `items`, not `datasets`** (`DatasetManager.list_datasets`'s actual return shape) — a stale doc/consumer that reads `datasets` gets `undefined` silently rather than an error and just renders an empty list (fixed in the merge panel after shipping exactly this bug — commit `ae19569`). Only `dataset_kind=STANDARD` datasets are listed here; the LIVE dataset is reached through `/api/live`. Each item includes: id, name, region, dates, status, source_configs (array), fusion_strategy, `scenes_by_source`/`bytes_by_source` (one aggregate query for the whole page, not per-card), total_size_bytes.

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
→ 200 { "dataset_id": 7, "status": "PROCESSING", "progress_percent": 45, "scenes": [...] }
```

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
Always 200 with an empty gallery for a dataset that has no PREVIEW output yet — that is a normal state, not an error. A scene with both RAW and PROCESSED configured for the same source carries **two** levels under `by_level`; top-level `kinds` mirrors whichever level `preferred_preview_level()` picks (PROCESSED first).

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
Canonical D14 tier names as keys. `legacy_layout: true` means this dataset predates the D15 relayout (`{YYYYMMDD}/{tier}/{source}/` instead of `{source}/{RAW|PROCESSED}/`) and the Struktur panel shows a "format lama" notice instead of a tree.

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

#### Get Last Configuration (for "Clone Last Config" feature)
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

Returns the most recently created dataset's configuration (region, sources with processing levels, fusion strategy, preview options). Used by frontend's "Pakai Config Sebelumnya" button to pre-populate the creation wizard.

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

Note: `api/routes/preview.py` (`/api/preview/*`, thumbnail-on-the-fly from COG) is **not mounted** in `api/main.py` — the gallery reads PREVIEW-tier PNGs through `/api/datasets/{id}/preview` instead, to avoid two different render/stretch definitions of "preview".

### Live Ingestion

```
GET  /api/live                          → { "dataset_id", "enabled", "status", "required_tiers", "bbox_wkt",
                                              "total_size_bytes", "last_checked_at", "sources": [...] }  (404 if no live dataset yet)
POST /api/live/toggle?enabled=true      → { "status": "..." }
POST /api/live/clear                    → { "status": "CLEARED", "freed_bytes", "deleted_count" }
POST /api/live/backfill                 → { "date_start": "...", "date_end": "..." } → { "status", "job_id", "date_range" }
GET  /api/live/scenes?limit=10          → [ { "product_id", "scene_date", "tier", "size_mb" } ]
```
Live is a single distinguished dataset (`dataset_kind="LIVE"`), not a list — `GET /api/live` 404s until it has been created (implicitly, the first time it's toggled on). Cron: daily 02:00 Asia/Jakarta via APScheduler.

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
{ "detail": "Human-readable message (Bahasa Indonesia in most newer routes)" }
```
There is no `code`/`details` envelope — `error`/`code` fields described in earlier drafts of this document were never implemented; `web/app.js` reads `detail` directly. Two endpoints have custom handlers that keep the same `detail` key: `422` (Pydantic validation errors are flattened from FastAPI's default list-of-objects into one string, joined `"field: message | field2: message2"`) and the catch-all `500` (`{"detail": "Internal server error", "path": "..."}`, logged server-side with the full traceback).

| HTTP | Typical cause |
|---|---|
| 400 | Invalid request body/query (e.g. bad tier name, `fusion_strategy` required, fewer than 2 `dataset_ids` for a merge) |
| 403 | Attempted write on a `SEEDER` region |
| 404 | Resource doesn't exist, or exists but the requested sub-resource hasn't been produced yet (e.g. quality metrics before module6 ran) |
| 409 | Duplicate name, or overwriting existing output without `overwrite=true` |
| 422 | Pydantic request validation failure |
| 500 | Unhandled server error |
| 503 | Database connection failed |

---

## Web UI

### Technology

Vanilla HTML5 + CSS3 + JavaScript. Leaflet.js for maps. No build step, no framework. Served as static files by FastAPI. Fonts: IBM Plex Mono (headings, labels, monospace data) + IBM Plex Sans (body text).

### Visual Design System

#### Color Palette

```css
--ink: #0A0E1A;          /* deepest background */
--panel: #121A2B;         /* panel fill (not used directly — glass-bg replaces it) */
--panel-alt: #182238;     /* input fields, alternate surfaces */
--hairline: #232F49;      /* borders, dividers */
--text: #E7ECF5;          /* primary text */
--text-dim: #00F0FF;      /* labels, secondary text (cyan, not gray) */
--cyan: #00F0FF;          /* accent: active states, status, links */
--amber: #F0A63C;         /* warning badges */
--coral: #EF6461;         /* danger/error badges, delete buttons */
```

#### Glassmorphism Surface (Every Panel, Card, Modal, Toast, Navbar)

Every container uses this stack — no solid backgrounds anywhere:

```css
background: rgba(10,14,26,0.16);           /* ~16% opacity black */
backdrop-filter: blur(22px) saturate(140%); /* blur what's behind */
border: 2px solid transparent;              /* invisible — gradient goes in ::before */
overflow: hidden;                           /* clip blur to rounded corners */
box-shadow:
  inset -4px -4px 10px rgba(0,0,0,0.35),   /* inner depth: bottom-right dark */
  inset 4px 4px 10px rgba(255,255,255,0.05),/* inner depth: top-left light */
  0 8px 32px rgba(0,0,0,0.28);             /* outer shadow */
```

#### Gradient Border (::before Pseudo-Element)

Every glass surface has a gradient border rendered via a masked `::before`:

```css
.glass::before {
  content: '';
  position: absolute;
  inset: 0;
  border-radius: inherit;
  padding: 2px;                              /* border thickness */
  background: linear-gradient(135deg,
    rgba(0,240,255,0.9) 0%,                  /* cyan at top-left */
    rgba(10,14,26,0.9) 65%                   /* fades to near-black at bottom-right */
  );
  -webkit-mask: linear-gradient(#fff 0 0) content-box,
                linear-gradient(#fff 0 0);
  -webkit-mask-composite: xor;
  mask-composite: exclude;                   /* punches hole → only border visible */
  pointer-events: none;
}
```

#### Hover Glow

Cards, panels, and sections gain a cyan glow on hover:

```css
.card:hover {
  background: rgba(10,14,26,0.08);          /* slightly more transparent */
  box-shadow:
    inset -4px -4px 10px rgba(0,0,0,0.35),
    inset 4px 4px 10px rgba(255,255,255,0.05),
    0 8px 32px rgba(0,0,0,0.28),
    0 0 46px rgba(0,240,255,0.4);           /* ← cyan glow ring */
}
```

#### Border Radius

- Panels, side sections: `18px`
- Cards, modals, inputs, map gap: `10px` (`--radius`)
- Pills, badges, chips: `999px` (capsule)

#### Background

The entire app background is a **full-viewport Leaflet map** (`position: fixed; inset: 0; z-index: -2`). On top sits a radial-gradient overlay for ambient color. Outside the "Buat Dataset" tab, the overlay adds a dark linear gradient so cards remain readable over bright map tiles.

### Floating Navbar (Expanding Pill)

The navbar is a **compact circular pill** (56px wide, showing only the logo) that expands when the user hovers anywhere in the top 1/6 of the viewport:

```
COLLAPSED (default):
┌──────┐
│ LOGO │  ← 56px pill, border-radius: 999px
└──────┘

EXPANDED (on hover / focus / click):
┌──────────────────────────────────────────────────────────────────┐
│ LOGO  THE TRINITY          Buat Dataset │ Dataset Saya │ Live  ● │
│        SENTINEL/MODIS/GPM                                        │
└──────────────────────────────────────────────────────────────────┘
```

- Trigger zone: `position: fixed; top: 0; width: 100vw; height: 16.667vh`
- Transition: `max-width 0.4s cubic-bezier(.4,0,.2,1)` — brand text, tabs, and status indicator fade in with `opacity` transition + `transition-delay: 0.15–0.2s`
- Active tab: `color: var(--ink); background: var(--cyan)` (black text on cyan fill)
- Status dot: 8px circle — `.ok` = cyan with glow, `.degraded` = amber, `.down` = coral

### Tab Structure

```
┌─────────────────┬──────────────────┬──────────────┐
│  Buat Dataset   │  Dataset Saya    │  Live        │
│  (Create)       │  (My Datasets)   │  (Ingestion) │
└─────────────────┴──────────────────┴──────────────┘
```

### Tab 1: Buat Dataset (Create Dataset)

#### Layout: Three-Column Grid

```
┌────────────┐  ┌──────────────────┐  ┌────────────┐
│  Pilih     │  │                  │  │ Konfigurasi│
│  Lokasi    │  │   Peta Wilayah   │  │            │
│            │  │   (map gap —     │  │ Dates      │
│ [+Tambah]  │  │    transparent   │  │ Satellites │
│ [🔍Cari]   │  │    hole showing  │  │ Processing │
│            │  │    Leaflet map)  │  │ Fusion     │
│ Card list  │  │                  │  │ Preview    │
│ of saved   │  │   Selection box  │  │ Name       │
│ regions    │  │   (cyan border)  │  │            │
│            │  │                  │  │ [Buat]     │
└────────────┘  └──────────────────┘  └────────────┘
  ~268px          flexible width        ~268px
```

Both side panels are glass surfaces (18px radius) with internal scrolling (`.panel-scroll`). The map gap is fully transparent — it's a hole in the layout revealing the background Leaflet map, with a gradient border `::before` and an SVG mask (`#mapMask`, `fill-rule: evenodd`) blocking pointer events outside the gap so the map can only be dragged inside the window.

**Selected region** shown as a cyan-bordered rectangle on the map, repositioned on every map move via `latLngToContainerPoint`.

#### Pre-Wizard: Clone Last Configuration

Above the 4-step wizard, a "Pakai Config Sebelumnya" button (gear icon + text) appears only if user has previously created a dataset:

```
┌────────────────────────────────────────┐
│ [⚙ Pakai Config Sebelumnya]            │  ← Click to populate
│                                        │
│ Konfigurasi Terakhir:                  │
│ • Jabodetabek                          │
│ • Tanggal: 2026-08-01 s/d 2026-08-31   │
│ • S1[RAW+PROC] · MODIS[PROC] · GPM[RAW]│
│ • Strategi: HYBRID                     │
└────────────────────────────────────────┘
```

On click:
1. Fetch `GET /api/datasets/last-config`
2. Populate form fields (region, date range, source checkboxes, fusion strategy, preview options)
3. Focus on first field for editing
4. User can edit before creating (dates, region, anything) -- date range is a preset, not a lock

If no prior datasets exist, button hidden. If API fails, button stays visible but shows inline error.

#### User Journey (4-Step Wizard in Right Panel)

**Step 1 — Region & Date**
- Location: preset region cards (from `regions_of_interest` DB table) + free-text geocoding via Nominatim + Leaflet map
- Region card: glass surface, selected state = `rgba(0,240,255,0.20)` bg + cyan left-edge bar (`::after`) + glow shadow
- Date range: two `input[type=date]` fields

**Step 2 — Satellite & Processing Selection** (combined — each satellite has its own processing config)

Top-level master toggle:
```
Sumber Data Satelit:                              [☑ Pilih Semua]
```
"Pilih Semua" checks all 3 sources + all their processing levels.

Each satellite is an expandable `.option-row` card. Enabling a source reveals its per-satellite processing checkboxes:

```
┌─────────────────────────────────────────────────────────────┐
│ ☑ Sentinel-1 SAR (ESA)                                     │
│   radar, all-weather, ~10m, revisit ~7-8 hari               │
│                                                             │
│   Tingkat Pemrosesan:                        [☑ Semua]      │
│     ☑ RAW  — kalibrasi + crop (tanpa Lee filter, tanpa QA)  │
│     ☑ PROCESSED — + Lee filter 7×7 + QA analytics + COG     │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│ ☐ MODIS Optical (NASA)                        [disabled]    │
│   flood/vegetation, 250m, daily                             │
│                                                             │
│   Tingkat Pemrosesan:                        [☐ Semua]      │
│     ☐ RAW  — flood map saja (tanpa indeks turunan)          │
│     ☐ PROCESSED — + hitung NDVI + NDWI dari reflectance     │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│ ☐ GPM IMERG Rainfall (NASA/JAXA)              [disabled]    │
│   precipitation, ~10km, daily                               │
│                                                             │
│   Tingkat Pemrosesan:                        [☐ Semua]      │
│     ☐ RAW  — curah hujan harian (hari itu saja)             │
│     ☐ PROCESSED — + akumulasi 24h / 72h / 7 hari            │
└─────────────────────────────────────────────────────────────┘
```

Key interactions:
- Unchecking a source collapses its processing options and grays them out
- Each source has its own "Semua" mini-toggle for its processing levels
- The top-level "Pilih Semua" checks all sources + all processing levels
- At least 1 source with at least 1 processing level is required to proceed

**Step 3 — Fusion & Preview**

Sub-step 3a — Fusion Strategy (shown only if >1 source enabled):
```
Strategi Fusi:
  ○ CO-OCCURRENCE — hanya tanggal yang semua sumber punya data
  ○ FULL_COVERAGE — setiap hari, ±1-2 hari offset OK
  ○ HYBRID — auxiliary harian, S1 jadi jangkar
```

Sub-step 3b — Preview Options (optional, `.option-row` cards):
```
  ☐ GRAYSCALE — percentile 2-98 stretch
  ☐ COLORED — per-source colormap
  ☐ COMPOSITE — false color RGB (khusus S1)
```

The `.option-row` component: `border: 1px solid var(--glass-border); border-radius: 12px; padding: 11px 13px`. When checked: `border-color: rgba(0,240,255,0.45); background: rgba(0,240,255,0.07)` (uses `:has(input:checked)`).

**Step 4 — Review & Create**
- Dataset name input, optional description textarea
- "Buat Dataset" button (`.btn-primary`: cyan bg, black text, full-width)

#### Validation Rules
- At least 1 source must be enabled with at least 1 processing level checked
- Each enabled source must have at least 1 processing level checked
- Fusion strategy required if >1 source enabled, hidden if only 1
- Date range valid (start ≤ end)
- Region must be selected from list

### Tab 2: Dataset Saya (My Datasets)

#### Dataset Card Layout

```
┌──────────────────────────────────────────────────────────────┐
│  Dataset Name                              PROCESSING        │
│  Region · Date Range                                         │
│  ┌──────────┬──────────┬──────────┐                          │
│  │ S1[R+P]  │ MODIS[P] │ GPM[R]   │  ← per-source processing│
│  └──────────┴──────────┴──────────┘                          │
│                                                              │
│  Strategi Fusi: HYBRID                                       │
│  S1: 5 scene · MODIS: 30 scene · GPM: 30 scene              │
│  Storage: 2.4 GB (S1: 1.2G | MODIS: 0.8G | GPM: 0.4G)      │
│                                                              │
│  Live Logs (terbaru)                                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ 14:02:15  S1A_IW...  LEE_FILTER  COMPLETED          │   │
│  │ 14:01:58  S1A_IW...  CROP        COMPLETED          │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  [Jeda] [Lanjutkan] [Coba lagi] [Batalkan] [Unduh] [Hapus] │
│  [Detail] [Struktur]                                        │
└──────────────────────────────────────────────────────────────┘
```

**Key changes from Prototype**:
- ❌ Removed tier chips (tier bukan pilihan user — lihat D14)
- ✅ Source + processing level chips: `S1[R+P]`, `MODIS[P]`, `GPM[R]`
  - `R` = RAW configured, `P` = PROCESSED configured, `[R+P]` = both selected
  - Only show sources actually configured in dataset
- ❌ Removed ring progress with tier colors
- ✅ Fusion strategy label, plus "hasil fusi saja" when `fusion_output_only` is set
- ✅ Live logs still show per-stage progress
- ✅ Per-source scene count and byte breakdown (`scenes_by_source`,
  `bytes_by_source` on `DatasetItem`). Computed from `data_products` with ONE
  aggregate query for the whole listing page — the card re-renders on every
  poll, so a per-dataset fan-out would multiply database load by the number of
  cards on screen. Scenes are counted DISTINCT: one scene produces many product
  rows (VV, VH, several tiers), so counting rows would report a multiple of the
  real figure. Sources with no products yet are absent rather than zero, so the
  card can tell "belum ada" from "nol byte".

**Card styling**: `.dataset-card` (glass surface, 18px radius, cyan border gradient on hover)

#### Expandable Panels

**Detail panel**:
- Scene list per source (S1 scenes, MODIS granules, GPM daily)
- Per-scene: product_identifier, current stage, status, last error
- Scene status colors: COMPLETED = cyan, RUNNING = amber, FAILED = coral

**Struktur panel**:
- Storage rows per tier, each split into per-source segments. **Labels show the
  DRAWER, not the tier name** (`tierLabel()` in `web/app.js`): `ALIGNED` renders
  as `RAW`, `COG` as `PROCESSED`, `FUSED` as `FUSION` — because that is what the
  user sees when they open the downloaded folder (`sentinel-1/RAW/`,
  `sentinel-1/PROCESSED/`). The `?tier=` value in the download link keeps the
  real tier name so the URL still resolves.
  ```
  RAW          700 B   1 berkas · 1 scene   [Berkas] [Unduh]   (granule cache)
  RAW        1.00 KB   1 berkas · 1 scene   [Berkas] [Unduh]   ← tier ALIGNED
  PROCESSED  2.50 KB   2 berkas · 1 scene   [Berkas] [Unduh]   ← tier COG
  FUSION     3.00 KB   1 berkas · 1 scene   [Berkas] [Unduh]   ← tier FUSED
  ```
- Datasets created before the relayout are **not rendered as a tree at all**:
  `storage.legacy_layout` is true and the panel shows a "format lama" notice
  plus a whole-dataset download link. Their folder vocabulary no longer matches
  anything else on screen.
- Quality metrics per source: nodata%, speckle, score (served by
  `/api/quality/dataset/{id}/by-source`, which is rank-3 based)
- ✅ Collapsible nesting source → level → tier, built with native `<details>`
  (open/close needs no JS state). The middle layer is **derived client-side**
  from the tier via `TIER_LEVEL` — the disk has no level segment outside
  `preview/`. A source with only one configured level collapses that layer
  away: a single-child node adds a click without adding information.
- ✅ File browser filtered by source (`/storage/files/{tier}?source=`, filtered
  server-side) and grouped by date. Source and tier are already fixed by the
  leaf that was clicked, so the columns are Tanggal | Scene | Berkas | Ukuran.
  The date is read from the filename, since it is no longer a path segment.

#### Preview Gallery (Inside Struktur Panel)

Structure:
```
SENTINEL-1 RAW          [Date selector: 2024-01-15 ↕]
Grayscale / Colored
[Image grid 190px min]

SENTINEL-1 PROCESSED   [Date selector: 2024-01-15 ↕]
Grayscale / Colored
[Image grid 190px min]

MODIS PROCESSED        [Date selector: 2024-01-15 ↕]
Grayscale / Colored
[Image grid 190px min]
```

Each image card: thumbnail + label (band name), colormap tag, value range, interpretation note (3 lines max). Checkerboard for NoData.

#### Merge Panel (Above the Dataset List)

`data-action="toggle-merge"`, backed by `/api/merge/candidates` and `/api/merge/run` (see "Merge" above, DOCS/DECISIONS.md D19). Shown only when at least one date has stacks in more than one dataset — the normal case when an AOI was split into strips (D16).

A single accordion (not one per row, since a "Gabungkan Dataset" decision spans datasets, not one card). Deliberately fetched only from `loadDatasets()` (opening the tab, pressing Refresh, finishing a merge) and **not** from the 10-second `refreshProgress` poll: `GET /api/merge/candidates` opens every FUSION HDF5 on disk to check its attributes, which is too expensive to run every poll cycle for a candidate list that only changes when a new fusion stack appears — a minutes-to-hours cadence:

```
┌ Gabungkan Dataset ───────────────────── 4 tanggal bisa digabung  [▾] ┐
│                                                                      │
│  2025-12-04    JAWA_A · JAWA_B · JAWA_C · JAWA_D                     │
│                14 stack · 103630 x 32040 px · 8 lapisan · ~22 GB     │
│                                                       [Gabungkan]     │
│                                                                      │
│  2025-12-06    JAWA_A · JAWA_B                                       │
│                Sudah digabung · 5.1 GB    [Gabung Ulang] [Hapus]     │
│                [thumbnail] [thumbnail] [thumbnail]                   │
└──────────────────────────────────────────────────────────────────────┘
```

- Each row is one date with mergeable stacks across ≥2 datasets, showing stack count, output shape, layer count, and total input size. A row not yet merged shows a **Gabungkan** button; an already-merged row shows **Gabung Ulang** (re-merge with `overwrite=true`), a **Hapus** button, and its existing preview thumbnails. Rows with a `warnings` entry from the API show it inline (`merge-warn`).
- **The API includes blocked (non-mergeable) candidates with their `blocked_reason`** (see REST API "Merge" above), but `renderMergePanel()` in `web/app.js` filters `data.candidates` down to `c.mergeable` before rendering anything — the panel only ever shows rows that can actually be merged right now, not near-misses. A caller of `GET /api/merge/candidates` directly does see the blocked ones.
- Clicking **Gabungkan**/**Gabung Ulang** calls `POST /api/merge/run` with the exact `dataset_ids` shown in that row, not a re-derived candidate set, so the merge always matches what the user was looking at when they clicked.
- An already-merged row with no preview images yet (output merged before preview rendering existed) shows a **Buat Preview** button instead of thumbnails, hitting `POST /api/merge/preview/{date}/rebuild` without re-merging. Thumbnails, once present, link to `/api/merge/preview/{date}/{filename}`.
- **Hapus** calls `DELETE /api/merge/result/{date}` after a confirm() dialog — deletes only the merged HDF5 + its previews from `data/merged/`, never the source per-dataset FUSION stacks, so the row can always be regenerated with **Gabungkan** afterwards.
- Merged output is never attributed to any one dataset card — it has its own row here because it spans several datasets.

### Tab 3: Live (Live Ingestion)

- Master toggle (`.switch` — capsule slider, cyan when ON)
- Per-source enable rows (dot indicator + source name + processing level + last check/ingest timestamps)
  - E.g., "Sentinel-1 [RAW+PROCESSED] · Last check: 2 hours ago · Last ingest: 1 hour ago"
- Total storage size, overall last checked timestamp
- Backfill form (date range + submit)
- Recent ingested scenes table (`GET /api/live/scenes`, `loadLiveScenes()` in `web/app.js`), three columns only — the response (`LiveSceneItem`) doesn't carry source/stage/status, just tier:
  | Tanggal | Tier | Ukuran |
  |---|---|---|
  | 2024-09-08 14:02 | PROCESSED | 125.0 MB |
  | 2024-09-07 09:15 | RAW | 65.0 MB |

### Modals

Glass surfaces (`border-radius: 10px; padding: 24px; width: 360px`), backdrop `rgba(6,9,16,0.55)` + `blur(4px)`. Used for:
- Add location (2-tab: search via Nominatim / manual coordinates)
- Delete location (soft-delete confirmation)
- Delete dataset (with force-stop checkbox)
- Cancel dataset
- Clear live data

All close on Escape key.

### Toasts

Bottom-right stack. Glass surface with gradient border. Success = cyan tint, Error = coral tint. Auto-dismiss after 4.2s with `translateY` entrance animation.

### UI Language

Primary: **Indonesian (Bahasa Indonesia)**. All labels, tooltips, status messages.

Status labels: Antrian (Queued), Sedang Diproses (Processing), Selesai (Completed), Gagal (Failed), Dijeda (Paused), Dibatalkan (Cancelled).

### Responsive Behavior

- **≤900px**: Single-column stack, map gap hidden, navbar trigger zone shrinks to 92px fixed height
- **≤1180px**: Narrower side panels (232px), shorter map window
- **≤560px**: Preview grid columns narrow to `minmax(140px, 1fr)`, option-row padding tightens
- `prefers-reduced-motion`: All animations and transitions disabled
