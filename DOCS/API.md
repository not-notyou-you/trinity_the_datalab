# API Reference

**Base URL**: `http://localhost:8000/api/`
**Format**: JSON. **Auth**: None (add before public exposure). **CORS**: `*` (restrict before production).

## Health

```
GET /api/health
→ 200 { "status": "healthy", "db_connected": true, "pool": {...}, "timestamp": "..." }
```

## Datasets

### Create Dataset
```
POST /api/datasets
```
```json
{
  "location": "Jabodetabek",
  "date_start": "2024-01-01",
  "date_end": "2024-01-31",
  "name": "Ablation Study Jan 2024",
  "sources": {
    "sentinel1": { "processing": ["RAW", "PROCESSED"] },
    "modis": { "processing": ["PROCESSED"] },
    "gpm": { "processing": ["RAW"] }
  },
  "fusion_strategy": "HYBRID",
  "preview_options": ["COLORED", "COMPOSITE"]
}
```

**`sources` object**:
- Keys: `sentinel1`, `modis`, `gpm`. Include only sources to ingest (omitted = not ingested).
- `processing`: array of 1–2 values from `["RAW", "PROCESSED"]`. Required per included source.
- At least 1 source must be included.
- What RAW/PROCESSED means differs per satellite (see ETL.md).

**Validation**:
- `sources`: At least 1 key required. Each key must have non-empty `processing` array.
- `fusion_strategy`: Required if `sources` has >1 key, must be null/omitted if only 1 key. Values: `CO_OCCURRENCE`, `FULL_COVERAGE`, `HYBRID`. See DOCS/ETL.md "Strategies: two axes, not one" — the strategy controls **both** which aux dates are downloaded and which dates become an HDF5.
- `fusion_output_only` (bool, default `false`): keep only the fusion HDF5s; per-satellite artifacts are deleted **after** each date's stack is written. Not a "skip processing" switch — fusion reads those artifacts, so they are still built. Rejected with 400 if no `fusion_strategy` is set, since that would leave the dataset empty.
- `s1_match_tolerance_days` (int 0–14, default `2`): `FULL_COVERAGE` only. How far it may borrow a Sentinel-1 scene from a neighbouring date. `0` means same-day only. Days with no scene in range still produce a file, with the `sentinel1/` group filled with NaN; the gap actually used is recorded in `fusion_products.s1_offset_days` (NULL when there was no scene at all).
- `preview_options`: Optional. Values: `GRAYSCALE`, `COLORED`, `COMPOSITE`.
- `location`: Preset name, free-text (geocoded), or `"lat1,lon1,lat2,lon2"` bbox string.

```
→ 201 {
    "dataset_id": 7,
    "job_id": "uuid",
    "status": "QUEUED",
    "source_configs": [
      { "source": "sentinel1", "processing": ["RAW", "PROCESSED"] },
      { "source": "modis", "processing": ["PROCESSED"] },
      { "source": "gpm", "processing": ["RAW"] }
    ]
  }
→ 400 { "error": "fusion_strategy required when multiple sources configured" }
→ 400 { "error": "sources.modis.processing must contain at least one value" }
```

### List Datasets
```
GET /api/datasets?limit=10&offset=0
→ 200 { "datasets": [...], "total": 42, "offset": 0, "limit": 10 }
```
Each dataset includes: id, name, region, dates, status, source_configs (array), fusion_strategy, scene counts, total_size_bytes.

### Get Dataset Detail
```
GET /api/datasets/{id}
→ 200 { full dataset metadata + source_configs + scene list + product list }
```

### Dataset Status
```
GET /api/datasets/{id}/status
→ 200 { "dataset_id": 7, "status": "PROCESSING", "progress_percent": 45, "scenes": [...] }
```

### Pause / Resume / Cancel / Retry
```
POST /api/datasets/{id}/pause    → 200 { "status": "PAUSED" }
POST /api/datasets/{id}/resume   → 200 { "status": "PROCESSING" }
POST /api/datasets/{id}/cancel   → 200 { "status": "CANCELLED" }
POST /api/datasets/{id}/retry    → 200 { "status": "PROCESSING", "retried_scenes": 3 }
```

### Logs
```
GET /api/datasets/{id}/logs?stage=LEE_FILTER&status=FAILED&limit=20
→ 200 { "logs": [{ "log_id", "timestamp", "stage", "scene_id", "status", "duration_sec", "error_type", "error_message" }] }
```

### Download
```
GET /api/datasets/{id}/download
→ 200 (streaming ZIP)
```

### Storage Summary
```
GET /api/datasets/{id}/storage/summary
→ 200 { "dataset_id": 7, "total_size_bytes": ..., "tier_summary": { "raw": {...}, "gold": {...}, "fusion": {...} } }
```

### Delete
```
DELETE /api/datasets/{id}?delete_files=true
→ 200 { "deleted": true }
```

### Get Last Configuration (for "Clone Last Config" feature)
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
    "preview_options": ["COLORED", "COMPOSITE"],
    "date_start": "2026-08-01",
    "date_end": "2026-08-31",
    "created_from_dataset_id": 42,
    "created_at": "2026-09-06T14:08:51Z"
  }
→ 404 { "error": "No dataset found yet" }
```

Returns the most recently created dataset's configuration (region, sources with processing levels, fusion strategy, preview options). Used by frontend's "Pakai Config Sebelumnya" button to pre-populate the creation wizard.

## Scenes

```
GET /api/scenes?dataset_id=7&date_from=2024-01-01&date_to=2024-01-31
GET /api/scenes/{scene_id}
GET /api/scenes/{scene_id}/status
```

## Products

```
GET /api/products?dataset_id=7&tier=GOLD&source=SENTINEL1&processing_level=PROCESSED
GET /api/products/{id}
GET /api/products/{id}/download   → binary file
GET /api/products/{id}/verify     → { "valid": true, "checksum_expected": "...", "checksum_actual": "..." }
```

Filter `processing_level=RAW|PROCESSED` retrieves artifacts from a specific processing variant.
Filter `source=SENTINEL1|MODIS|GPM` narrows to one satellite.

## Quality

```
GET /api/quality/{scene_id}
→ 200 { "metrics": [{ "band_name": "VV", "quality_flag": "PASS", "quality_score": 82, ... }] }
```

Note: Quality metrics are only produced for Sentinel-1 PROCESSED (Lee filter + QA stage). MODIS and GPM do not have radiometric QA scoring.

## Lineage

```
GET /api/metadata/lineage/{product_id}
→ 200 { "chain": [{ "source_product_id", "target_product_id", "transformation_type", "checksums" }] }
```
Recursive: returns full provenance tree from RAW → FUSION. `transformation_type` values include: `CALIBRATE`, `CROP`, `LEE_FILTER`, `QUALITY_ANALYTICS`, `GOLD_EXPORT`, `COMPUTE_NDVI`, `COMPUTE_NDWI`, `ACCUMULATE_RAIN`, `FUSE`.

## Preview

```
GET /api/datasets/{id}/preview
GET /api/datasets/{id}/preview/{date}/{kind}/{filename}
```
`kind`: `grayscale`, `colored`, or `composite`.

## Live Ingestion

```
GET  /api/live                        → current config + status
POST /api/live/toggle?enabled=true    → enable/disable
POST /api/live/backfill               → { "date_start": "...", "date_end": "..." }
GET  /api/live/scenes?limit=10        → recent live-ingested scenes
```

## Regions

```
GET /api/regions
→ 200 { "regions": [{ "region_id": 1, "name": "Jabodetabek", "bbox": "POLYGON(...)" }] }
```

## Error Response Format

All errors follow:
```json
{ "error": "Human-readable message", "code": "ERROR_CODE", "details": {} }
```

| HTTP | Code | Meaning |
|---|---|---|
| 400 | `VALIDATION_ERROR` | Invalid request body |
| 404 | `NOT_FOUND` | Resource doesn't exist |
| 409 | `CONFLICT` | Dataset already processing / duplicate |
| 500 | `INTERNAL_ERROR` | Server error |
| 503 | `DB_UNAVAILABLE` | Database connection failed |
